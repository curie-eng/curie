"""The GitHub forge fake models GitHub's pull request review API (#5794 decision 10).

Every behaviour asserted here is GitHub's documented one, cited where it is
checked; the factory's own review admission is tested elsewhere against this
fake, so these tests pin that the fake tells the truth about GitHub.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.gitflow import verify_signature
from curie_api.github_review_events import FeedbackIgnored, parse_feedback
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from forge_fakes.github import (
    REPO,
    REPO_ID,
    GitHubRepositoryFake,
    WebhookDelivery,
    post_delivery,
    signed_delivery,
)

SECRET = "review-fake-test-secret"
HTML = "https://github.com"
APP_TOKEN = "fixture-installation-token"
REVIEWER = {"id": 7101, "login": "reviewer", "type": "User"}
OTHER = {"id": 7102, "login": "second-reviewer", "type": "User"}
COMMENTS = [
    {"path": "src/widget.py", "line": 12, "body": "Rename this helper."},
    {"path": "README.md", "body": "Document the flag in this file."},
]


@pytest.fixture
def secret(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    with monkeypatch.context() as patched:
        patched.setenv("GITHUB_WEBHOOK_SECRET", SECRET)
        get_settings.cache_clear()
        yield SECRET
    get_settings.cache_clear()


@pytest.fixture
def fake() -> GitHubRepositoryFake:
    github = GitHubRepositoryFake()
    github.push_head("feature")
    github.seed_pull("feature", "main")
    github.grant(REVIEWER, "write")
    github.grant(OTHER, "maintain")
    return github


def _pull(fake: GitHubRepositoryFake) -> int:
    (number,) = fake.pulls
    return number


def _get(fake: GitHubRepositoryFake, token: str, path: str, **params: Any) -> httpx.Response:
    with fake.client_for(token) as client:
        return client.get(f"/repos/{REPO}{path}", params=params)


def _post(
    fake: GitHubRepositoryFake, token: str, path: str, body: dict[str, Any]
) -> httpx.Response:
    with fake.client_for(token) as client:
        return client.post(f"/repos/{REPO}{path}", json=body)


def _pending(fake: GitHubRepositoryFake, token: str) -> dict[str, Any]:
    created = _post(
        fake, token, f"/pulls/{_pull(fake)}/reviews", {"body": "Two things.", "comments": COMMENTS}
    )
    assert created.status_code == 200, created.text
    return created.json()


def _ids(items: list[dict[str, Any]]) -> list[int]:
    return [item["id"] for item in items]


def test_pending_review_is_visible_only_to_its_author(fake: GitHubRepositoryFake) -> None:
    # "To create a pending review for a pull request, leave the event parameter
    # blank"; pending reviews "do not include the submitted_at property":
    # https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
    # "Before you submit your review, your line comments are pending and only
    # visible to you":
    # https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/reviewing-proposed-changes-in-a-pull-request
    token = fake.user_token(REVIEWER)
    number = _pull(fake)
    review = _pending(fake, token)
    assert review["state"] == "PENDING"
    assert "submitted_at" not in review
    assert review["user"] == REVIEWER
    assert fake.deliveries == []
    comments = fake.comments_of_review(review["id"])
    assert [c["pull_request_review_id"] for c in comments] == [review["id"]] * 2
    since = "2000-01-01T00:00:00Z"

    # The author sees the review and its comments everywhere.
    assert _get(fake, token, f"/pulls/{number}/reviews/{review['id']}").json() == review
    assert _ids(_get(fake, token, f"/pulls/{number}/reviews").json()) == [review["id"]]
    listed = _get(fake, token, f"/pulls/{number}/reviews/{review['id']}/comments").json()
    assert _ids(listed) == _ids(comments)
    assert _get(fake, token, f"/pulls/comments/{comments[0]['id']}").status_code == 200
    assert _ids(_get(fake, token, "/pulls/comments", since=since).json()) == _ids(comments)

    # The App, and any other person, does not.
    for viewer in (APP_TOKEN, fake.user_token(OTHER)):
        assert _get(fake, viewer, f"/pulls/{number}/reviews/{review['id']}").status_code == 404
        assert _get(fake, viewer, f"/pulls/{number}/reviews").json() == []
        hidden = _get(fake, viewer, f"/pulls/{number}/reviews/{review['id']}/comments")
        assert hidden.status_code == 404
        for comment in comments:
            assert _get(fake, viewer, f"/pulls/comments/{comment['id']}").status_code == 404
        assert _get(fake, viewer, "/pulls/comments", since=since).json() == []
        assert _get(fake, viewer, f"/pulls/{number}/comments").json() == []


def test_submission_bumps_updated_at_not_created_at(fake: GitHubRepositoryFake) -> None:
    # Submit: https://docs.github.com/en/rest/pulls/reviews#submit-a-review-for-a-pull-request
    # Inline comments of a pending review keep created_at and get updated_at
    # bumped on submission (observed live, #2794).
    token = fake.user_token(REVIEWER)
    number = _pull(fake)
    review = _pending(fake, token)
    created = {c["id"]: c["created_at"] for c in fake.comments_of_review(review["id"])}
    fake.tick(8)

    submitted = _post(
        fake, token, f"/pulls/{number}/reviews/{review['id']}/events", {"event": "COMMENT"}
    )

    assert submitted.status_code == 200, submitted.text
    assert submitted.json()["state"] == "COMMENTED"
    stamp = submitted.json()["submitted_at"]
    assert all(stamp > at for at in created.values())
    app_read = _get(fake, APP_TOKEN, f"/pulls/{number}/reviews/{review['id']}")
    assert app_read.status_code == 200
    assert app_read.json()["submitted_at"] == stamp
    listed = _get(fake, APP_TOKEN, f"/pulls/{number}/reviews/{review['id']}/comments").json()
    assert {c["id"]: c["created_at"] for c in listed} == created
    assert {c["updated_at"] for c in listed} == {stamp}
    assert len(fake.deliveries) == 3
    # Submitting again, or without an event, is refused.
    path = f"/pulls/{number}/reviews/{review['id']}/events"
    assert _post(fake, token, path, {"event": "COMMENT"}).status_code == 422
    other = _pending(fake, fake.user_token(OTHER))
    other_path = f"/pulls/{number}/reviews/{other['id']}/events"
    assert _post(fake, fake.user_token(OTHER), other_path, {}).status_code == 422
    # Nobody but the author submits a pending review: it is not visible to them.
    assert _post(fake, APP_TOKEN, other_path, {"event": "COMMENT"}).status_code == 404


def test_review_and_comment_shapes(fake: GitHubRepositoryFake) -> None:
    # Review fields: https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request
    # Comment fields: https://docs.github.com/en/rest/pulls/reviews#list-comments-for-a-pull-request-review
    number = _pull(fake)
    head = fake._pull_payload(number)["head"]["sha"]
    posted = fake.create_review(number, REVIEWER, body="Look.", event="COMMENT", comments=COMMENTS)
    review = posted.item
    rid = review["id"]
    assert review["html_url"] == f"{HTML}/{REPO}/pull/{number}#pullrequestreview-{rid}"
    assert review["pull_request_url"] == f"https://api.github.com/repos/{REPO}/pulls/{number}"
    assert review["commit_id"] == head
    assert review["author_association"] == "COLLABORATOR"
    assert review["_links"]["html"]["href"] == review["html_url"]
    line, file = fake.comments_of_review(rid)
    for comment in (line, file):
        assert comment["html_url"] == f"{HTML}/{REPO}/pull/{number}#discussion_r{comment['id']}"
        assert comment["pull_request_url"] == review["pull_request_url"]
        assert comment["commit_id"] == comment["original_commit_id"] == head
        assert comment["user"] == REVIEWER
        assert "performed_via_github_app" not in comment
    assert (line["line"], line["subject_type"], line["path"]) == (12, "line", "src/widget.py")
    assert (file["line"], file["subject_type"]) == (None, "file")


def test_create_with_event_submits_immediately(fake: GitHubRepositoryFake) -> None:
    # "event": APPROVE, REQUEST_CHANGES or COMMENT submits in the creating call:
    # https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
    token = fake.user_token(REVIEWER)
    number = _pull(fake)
    created = _post(
        fake,
        token,
        f"/pulls/{number}/reviews",
        {"body": "Please fix.", "event": "REQUEST_CHANGES", "comments": COMMENTS},
    )
    assert created.status_code == 200, created.text
    review = created.json()
    assert review["state"] == "CHANGES_REQUESTED"
    assert review["submitted_at"]
    events = [(d.event, d.payload["action"]) for d in fake.drain_deliveries()]
    assert events == [
        ("pull_request_review", "submitted"),
        ("pull_request_review_comment", "created"),
        ("pull_request_review_comment", "created"),
    ]
    assert fake.deliveries == []
    for comment in fake.comments_of_review(review["id"]):
        assert comment["created_at"] == comment["updated_at"] == review["submitted_at"]


def test_deliveries_are_signed_and_parse_as_feedback(
    fake: GitHubRepositoryFake, secret: str
) -> None:
    # Delivery headers and X-Hub-Signature-256:
    # https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
    # Payloads: https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review
    # and #pull_request_review_comment; the consumer is parse_feedback.
    number = _pull(fake)
    pending = fake.create_review(number, REVIEWER, body="Two things.", comments=COMMENTS).item
    fake.tick(5)
    posted = fake.submit_review(number, pending["id"], "COMMENT")
    review_delivery, *comment_deliveries = posted.deliveries
    assert list(posted.deliveries) == fake.deliveries
    assert len(comment_deliveries) == 2
    parsed = []
    for delivery in posted.deliveries:
        assert delivery.headers["X-GitHub-Event"] == delivery.event
        assert verify_signature(secret, delivery.body, delivery.headers["X-Hub-Signature-256"])
        assert not verify_signature(
            secret, delivery.body + b" ", delivery.headers["X-Hub-Signature-256"]
        )
        payload = delivery.payload
        assert payload["installation"] == {"id": fake.installation_id}
        assert payload["repository"] == {"id": REPO_ID, "full_name": REPO}
        assert payload["sender"] == REVIEWER
        assert payload["pull_request"] == fake._pull_payload(number)
        parsed.append(
            parse_feedback(delivery.event, payload, delivery.delivery_id, github_html_base=HTML)
        )
    assert {feedback.review_id for feedback in parsed[1:]} == {pending["id"]}
    assert parsed[0].feedback_id == pending["id"]
    assert [(f.path, f.line) for f in parsed[1:]] == [("src/widget.py", 12), ("README.md", None)]
    assert all(f.head_sha == pending["commit_id"] for f in parsed)
    # REST states are uppercase; the webhook's review.state is lowercase.
    assert review_delivery.payload["review"]["state"] == "commented"
    rest = _get(fake, APP_TOKEN, f"/pulls/{number}/reviews/{pending['id']}").json()
    assert rest["state"] == "COMMENTED"
    assert {k: v for k, v in rest.items() if k != "state"} == {
        k: v for k, v in review_delivery.payload["review"].items() if k != "state"
    }


def test_counterexample_deliveries_name_what_they_carry(
    fake: GitHubRepositoryFake, secret: str
) -> None:
    # A comment whose pull_request_review_id names another review still parses,
    # but as that other review: the batch key comes from the delivery, so a
    # consumer that trusts it must be shown the right one.
    number = _pull(fake)
    first = fake.create_review(number, REVIEWER, body="One.", event="COMMENT", comments=COMMENTS)
    second = fake.create_review(number, OTHER, body="Two.", event="COMMENT")
    original = first.deliveries[1]
    payload = original.payload
    payload["comment"]["pull_request_review_id"] = second.item["id"]
    altered = signed_delivery(original.event, payload)
    assert verify_signature(secret, altered.body, altered.headers["X-Hub-Signature-256"])
    honest = parse_feedback(
        original.event, original.payload, original.delivery_id, github_html_base=HTML
    )
    moved = parse_feedback(
        altered.event, altered.payload, altered.delivery_id, github_html_base=HTML
    )
    assert honest.review_id == first.item["id"]
    assert moved.review_id == second.item["id"] != honest.review_id
    # An uppercase webhook state is what REST returns, not what webhooks send.
    assert first.deliveries[0].payload["review"]["state"] == "commented"
    assert first.item["state"] == "COMMENTED"
    # Without a pull_request_review_id the inline comment is refused.
    payload = original.payload
    del payload["comment"]["pull_request_review_id"]
    with pytest.raises(FeedbackIgnored, match="invalid_review_context"):
        parse_feedback(original.event, payload, original.delivery_id, github_html_base=HTML)


def test_approve_yields_approved(fake: GitHubRepositoryFake, secret: str) -> None:
    # https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
    token = fake.user_token(REVIEWER)
    number = _pull(fake)
    approved = _post(
        fake, token, f"/pulls/{number}/reviews", {"body": "Ship it.", "event": "APPROVE"}
    )
    assert approved.status_code == 200, approved.text
    assert approved.json()["state"] == "APPROVED"
    [delivery] = fake.drain_deliveries()
    assert delivery.payload["review"]["state"] == "approved"
    with pytest.raises(FeedbackIgnored, match="non_actionable_review"):
        parse_feedback(
            delivery.event, delivery.payload, delivery.delivery_id, github_html_base=HTML
        )
    # Dismissal: https://docs.github.com/en/rest/pulls/reviews#dismiss-a-review-for-a-pull-request
    dismissed = fake.dismiss_review(number, approved.json()["id"])
    assert dismissed.item["state"] == "DISMISSED"
    assert dismissed.deliveries[0].payload["action"] == "dismissed"
    assert dismissed.deliveries[0].payload["review"]["state"] == "dismissed"


def test_create_review_refusals(fake: GitHubRepositoryFake) -> None:
    # body is mandatory for COMMENT and REQUEST_CHANGES; the fake waives it only
    # for a review with inline comments.
    # https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
    token = fake.user_token(REVIEWER)
    path = f"/pulls/{_pull(fake)}/reviews"
    assert _post(fake, token, path, {"event": "COMMENT"}).status_code == 422
    assert _post(fake, token, path, {"event": "MERGE", "body": "x"}).status_code == 422
    assert _post(fake, token, path, {"comments": [{"path": "a.py"}]}).status_code == 422
    assert _post(fake, token, "/pulls/999/reviews", {"body": "x"}).status_code == 404
    only_comments = _post(fake, token, path, {"event": "COMMENT", "comments": COMMENTS[:1]})
    assert only_comments.status_code == 200
    assert only_comments.json()["body"] == ""
    # One pending review per person per pull request.
    assert _post(fake, token, path, {"body": "first"}).status_code == 200
    assert _post(fake, token, path, {"body": "second"}).status_code == 422


def test_edit_moves_updated_at(fake: GitHubRepositoryFake) -> None:
    # https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request
    token = fake.user_token(REVIEWER)
    number = _pull(fake)
    posted = fake.create_review(number, REVIEWER, body="Look.", event="COMMENT", comments=COMMENTS)
    comment = fake.comments_of_review(posted.item["id"])[0]
    fake.tick(30)
    with fake.client_for(token) as client:
        edited = client.patch(
            f"/repos/{REPO}/pulls/comments/{comment['id']}", json={"body": "Rename it, please."}
        )
    assert edited.status_code == 200, edited.text
    assert edited.json()["body"] == "Rename it, please."
    assert edited.json()["created_at"] == comment["created_at"]
    assert edited.json()["updated_at"] > posted.item["submitted_at"]
    fake.tick(30)
    again = fake.edit_comment(comment["id"], "Final words.")
    assert again["updated_at"] > edited.json()["updated_at"]


def test_listings_page_and_filter_by_since(fake: GitHubRepositoryFake) -> None:
    # per_page and page: https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
    # since on updated_at: https://docs.github.com/en/rest/pulls/comments#list-review-comments-in-a-repository
    number = _pull(fake)
    many = [{"path": f"f{n}.py", "line": n + 1, "body": f"Note {n}."} for n in range(3)]
    first = fake.create_review(number, REVIEWER, body="a", event="COMMENT", comments=many)
    fake.tick(60)
    second = fake.create_review(number, OTHER, body="b", event="COMMENT", comments=many[:1])
    fake.tick(60)
    third = fake.create_review(number, REVIEWER, body="c", event="APPROVE")
    pages = [
        _ids(_get(fake, APP_TOKEN, f"/pulls/{number}/reviews", per_page=2, page=page).json())
        for page in (1, 2, 3)
    ]
    assert pages == [[first.item["id"], second.item["id"]], [third.item["id"]], []]
    review_comments = f"/pulls/{number}/reviews/{first.item['id']}/comments"
    paged = [
        _ids(_get(fake, APP_TOKEN, review_comments, per_page=2, page=page).json())
        for page in (1, 2)
    ]
    assert [len(page) for page in paged] == [2, 1]
    assert paged[0] + paged[1] == _ids(fake.comments_of_review(first.item["id"]))
    since = first.item["submitted_at"]
    assert len(_get(fake, APP_TOKEN, "/pulls/comments", since=since).json()) == 4
    [late] = fake.comments_of_review(second.item["id"])
    since_late = _get(fake, APP_TOKEN, "/pulls/comments", since=late["updated_at"])
    assert _ids(since_late.json()) == [late["id"]]
    # An edit lists the comment again after its creation's window.
    fake.tick(60)
    edited = fake.edit_comment(paged[0][0], "Changed.")
    assert _ids(_get(fake, APP_TOKEN, "/pulls/comments", since=edited["updated_at"]).json()) == [
        paged[0][0]
    ]


def test_conversation_comment_by_a_person(fake: GitHubRepositoryFake, secret: str) -> None:
    # https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
    # https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment
    token = fake.user_token(REVIEWER)
    number = _pull(fake)
    created = _post(fake, token, f"/issues/{number}/comments", {"body": "Also rename main."})
    assert created.status_code == 201, created.text
    comment = created.json()
    assert comment["user"] == REVIEWER
    assert comment["html_url"] == f"{HTML}/{REPO}/pull/{number}#issuecomment-{comment['id']}"
    assert comment["created_at"] == comment["updated_at"]
    assert _get(fake, APP_TOKEN, f"/issues/comments/{comment['id']}").json() == comment
    listed = _get(fake, APP_TOKEN, "/issues/comments", since=comment["created_at"]).json()
    assert comment in listed
    [delivery] = fake.drain_deliveries()
    feedback = parse_feedback(
        delivery.event, delivery.payload, delivery.delivery_id, github_html_base=HTML
    )
    assert (feedback.event, feedback.feedback_id, feedback.review_id) == (
        "issue_comment",
        comment["id"],
        None,
    )
    # The App's own comment keeps the older shape the adapters rely on.
    app = _post(fake, APP_TOKEN, f"/issues/{number}/comments", {"body": "status"})
    assert app.status_code == 201
    assert app.json() == {"id": app.json()["id"]}


def test_legacy_helpers_stay_visible(fake: GitHubRepositoryFake) -> None:
    # add_review and add_review_comment keep serving the existing suites.
    number = _pull(fake)
    review = fake.add_review(number, REVIEWER, "Old style.")
    comment = fake.add_review_comment(number, REVIEWER, "Old inline.", app=False)
    assert _ids(_get(fake, APP_TOKEN, f"/pulls/{number}/reviews").json()) == [review["id"]]
    assert _get(fake, APP_TOKEN, f"/pulls/{number}/reviews/{review['id']}").status_code == 200
    assert _ids(_get(fake, APP_TOKEN, f"/pulls/{number}/comments").json()) == [comment["id"]]
    # Seeded without updated_at, they list whatever since says.
    since = "2999-01-01T00:00:00Z"
    assert _ids(_get(fake, APP_TOKEN, "/pulls/comments", since=since).json()) == [comment["id"]]


def test_tokens_are_read_from_every_authorization_form(fake: GitHubRepositoryFake) -> None:
    token = fake.user_token(REVIEWER)
    review = _pending(fake, token)
    path = f"https://api.github.com/repos/{REPO}/pulls/{_pull(fake)}/reviews/{review['id']}"
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    for header in (f"Bearer {token}", f"token {token}", f"Basic {basic}"):
        request = httpx.Request("GET", path, headers={"Authorization": header})
        assert fake.handle(request).status_code == 200, header
    assert fake.handle(httpx.Request("GET", path)).status_code == 404


def test_post_delivery_sends_the_exact_bytes(fake: GitHubRepositoryFake, secret: str) -> None:
    received: list[tuple[bytes, dict[str, str]]] = []
    app = FastAPI()

    @app.post("/github/webhook")
    async def webhook(request: Request) -> dict[str, str]:
        received.append((await request.body(), dict(request.headers)))
        return {"status": "ok"}

    posted = fake.create_review(_pull(fake), REVIEWER, body="Look.", event="COMMENT")
    delivery: WebhookDelivery = posted.deliveries[0]
    with TestClient(app) as client:
        assert post_delivery(client, delivery).status_code == 200
    [(body, headers)] = received
    assert body == delivery.body
    assert json.loads(body) == delivery.payload
    for name, value in delivery.headers.items():
        assert headers[name.lower()] == value
