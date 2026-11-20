"""A plain review comment on a factory pull request starts a revision on GitHub (#5794).

Every test drives a production caller: the signed ``POST /github/webhook``
route with the exact bytes and headers GitHub sends, or one ``poll_once`` pass.
GitHub is ``forge_fakes.github.GitHubRepositoryFake``, which models the
documented review API: a review created without ``event`` is pending and
visible only to its author, submission sets ``submitted_at`` and the state and
bumps each inline comment's ``updated_at``, and every inline comment names its
review as ``pull_request_review_id``:
https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
https://docs.github.com/en/rest/pulls/reviews#submit-a-review-for-a-pull-request
https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request
https://docs.github.com/en/rest/pulls/reviews#list-comments-for-a-pull-request-review
https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review
https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review_comment
https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment

Nothing inside the API is faked. Machine fixtures stand in for GitHub; they are
not human-authored GitHub proof (#5808 waives the live run).
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis.asyncio as aioredis
from curie_api.config import get_settings
from curie_api.factory_poll_intake import poll_once
from curie_api.main import create_app
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (  # noqa: E402
    APP_ID,
    LABEL,
    MENTION,
    REPO,
    REPO_ID,
    SENDER,
    SENDER_ID,
    GitHubRepositoryFake,
    Posted,
    WebhookDelivery,
    _issue_event,
    _post,
    post_delivery,
    signed_delivery,
)
from test_factory_poll_intake import _client_type, _patch_credentials, _poll_env  # noqa: E402
from test_github_factory_ingress import _code, _rows  # noqa: E402
from test_github_factory_review import _complete, _execute  # noqa: E402

pytestmark = pytest.mark.usefixtures("clean_db", "valkey")

PATHS = ("webhook", "poll")
REVIEWER = {"id": SENDER_ID, "login": SENDER, "type": "User"}
SECOND = {"id": 7302, "login": "second-reviewer", "type": "User"}
READER = {"id": 7303, "login": "read-only-reviewer", "type": "User"}
BOT = {"id": 7304, "login": "acme-helper", "type": "Bot"}
HTML = "https://github.com"
NOTICE = (
    "This pull request has had 10 revisions from plain comments. "
    f"To ask for another change, mention @{MENTION} in your comment."
)
NOTICE_MARKER = "curie-factory-mention-needed:"


def _review_key(pr: int, review_id: int) -> uuid.UUID:
    """The request id every piece of one submitted review admits under."""

    return uuid.uuid5(uuid.NAMESPACE_URL, f"curie-review:{REPO_ID}:{pr}:{review_id}")


def _review_url(pr: int, review_id: int) -> str:
    return f"{HTML}/{REPO}/pull/{pr}#pullrequestreview-{review_id}"


def _comment(path: str, line: int | None, body: str) -> dict[str, Any]:
    spec: dict[str, Any] = {"path": path, "body": body}
    if line is not None:
        spec["line"] = line
    return spec


def _provenance(objective: str) -> dict[str, Any]:
    provenance = json.loads(objective.rstrip("\n").rsplit("\n", 1)[1])
    assert isinstance(provenance, dict)
    return provenance


@dataclass
class Pull:
    number: int
    issue: int
    work_item_id: uuid.UUID
    lineage_id: uuid.UUID


@dataclass
class Factory:
    client: TestClient
    github: GitHubRepositoryFake

    # Intake doors ------------------------------------------------------------

    def post(self, delivery: WebhookDelivery) -> httpx.Response:
        response = post_delivery(self.client, delivery)
        assert response.status_code == 200, response.text
        return response

    def poll(self) -> None:
        async def go() -> None:
            engine = create_async_engine(get_settings().database_url)
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            try:
                async with httpx.AsyncClient() as http:
                    await poll_once(sessions, get_settings(), http)
            finally:
                await engine.dispose()

        asyncio.run(go())

    def reconcile(self) -> None:
        async def go() -> None:
            engine = create_async_engine(get_settings().database_url)
            maker = async_sessionmaker(engine, expire_on_commit=False)
            valkey = aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
            reconciler = WorkItemReconciler(maker, valkey, get_settings())
            try:
                await reconciler.run_once()
                await reconciler._sync_status_comments()
            finally:
                await valkey.aclose()
                await engine.dispose()

        asyncio.run(go())

    def deliver(self, path: str, posted: Posted) -> list[httpx.Response]:
        """Deliver what one action on GitHub produced, through ``path``."""

        self.github.drain_deliveries()
        if path == "poll":
            self.poll()
            return []
        return [self.post(delivery) for delivery in posted.deliveries]

    # Seeding -------------------------------------------------------------------

    def work_item(self) -> tuple[int, dict[str, Any]]:
        """An issue labelled by a writer and admitted through the signed webhook."""

        number = self.github.seed_issue("Rename the helper", "The helper name is unclear.")
        self.github.label_event(number, "labeled", LABEL, dict(REVIEWER))
        response = _post(
            self.client, "issues", _issue_event("labeled", number, label={"name": LABEL})
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", response.text
        rows = _requests(number)
        assert len(rows) == 1
        return number, rows[0]

    def pull(self, *, lineage_status: str = "open", complete: bool = True) -> Pull:
        """A factory-owned pull request, as a publication leaves it, with its
        first request finished."""

        number, first = self.work_item()
        branch = f"curie/publication-acme-{number}"
        head = self.github.push_head(branch)
        pr = self.github.seed_pull(branch, "main")
        lineage_id = _own_pull_request(
            first["work_item_id"], pr, branch=branch, head=head, status=lineage_status
        )
        if complete:
            _complete(self.client, first["id"])
        return Pull(pr, number, first["work_item_id"], lineage_id)

    def head(self, pull: Pull) -> str:
        return self.github.branches[self.github.pulls[pull.number]["head"]]

    def comment(self, pull: Pull, body: str, user: dict[str, Any] = REVIEWER) -> Posted:
        return self.github.add_conversation_comment(pull.number, user, body)

    def notices(self, pull: Pull) -> list[dict[str, Any]]:
        return [
            comment
            for comment in self.github.comments
            if comment["_number"] == pull.number and NOTICE_MARKER in comment["body"]
        ]


def _own_pull_request(
    work_item_id: uuid.UUID, pr: int, *, branch: str, head: str, status: str
) -> uuid.UUID:
    """Seed the lineage a factory publication leaves, with the fake's identity."""

    async def go() -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                item = (
                    (
                        await conn.execute(
                            text(
                                "SELECT agent_id, conversation_id FROM curie.work_items "
                                "WHERE id = :id"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                version_id, deployment_id, lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_versions (id, agent_id, version_label, "
                        "created_by) VALUES (:id, :agent, 'v1', 'fixture')"
                    ),
                    {"id": version_id, "agent": item["agent_id"]},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) VALUES "
                        "(:id, :agent, :version, CAST('dev' AS curie.environment), 'active')"
                    ),
                    {"id": deployment_id, "agent": item["agent_id"], "version": version_id},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.thread_publication_lineages "
                        "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                        "base_sha, branch, pr_number, pr_url, head_sha, status, version, "
                        "latest_revision, code_host_kind, code_host_host, "
                        "repository_project_id, code_host_installation_id, "
                        "code_host_pr_id, base_ref) VALUES "
                        "(:id, :agent, :deployment, :conversation, :repo, :base, :branch, "
                        ":pr, :url, :head, :status, 1, 1, 'github', 'github.com', :repo_id, "
                        ":installation, :node, 'main')"
                    ),
                    {
                        "id": lineage_id,
                        "agent": item["agent_id"],
                        "deployment": deployment_id,
                        "conversation": item["conversation_id"],
                        "repo": REPO,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "branch": branch,
                        "pr": pr,
                        "url": f"{HTML}/{REPO}/pull/{pr}",
                        "head": head,
                        "status": status,
                        "repo_id": str(REPO_ID),
                        "installation": 5501,
                        "node": f"PR_fixture_{pr}",
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.work_items SET publication_lineage_id = :lineage, "
                        "version = version + 1 WHERE id = :id"
                    ),
                    {"lineage": lineage_id, "id": work_item_id},
                )
                return lineage_id
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.sequence, r.status, r.objective, r.requester, r.reply_target_kind, "
        "r.reply_target_pr_number, r.reply_target_comment_id, r.reply_target_url, "
        "w.id AS work_item_id FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :repo AND w.tracker_issue_id = :number "
        "ORDER BY r.sequence",
        {"repo": str(REPO_ID), "number": str(number)},
    )


def _all_requests() -> int:
    return len(_rows("SELECT id FROM curie.execution_requests"))


def _lineage(pull: Pull) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT plain_feedback_revisions, plain_feedback_capped_at, updated_at, version "
        "FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": pull.lineage_id},
    )
    return row


def _codes(responses: list[httpx.Response]) -> list[str]:
    return [_code(response) for response in responses]


def factory_github(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> GitHubRepositoryFake:
    """Configure a polling GitHub factory whose every GitHub client reaches one fake."""

    for key, value in _poll_env(tmp_path, "poll").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", "factory-intake-worker")
    get_settings.cache_clear()
    _patch_credentials(monkeypatch)
    github = GitHubRepositoryFake()
    github.grant(REVIEWER, "write")
    github.grant(SECOND, "write")
    # Every GitHub client the API makes, the reconciler's included, reaches the fake.
    monkeypatch.setattr(httpx, "AsyncClient", _client_type(github))  # type: ignore[arg-type]
    return github


@contextmanager
def running_factory(github: GitHubRepositoryFake, *, new_agent: bool) -> Iterator[Factory]:
    """The API running against the configured database, optionally with a new
    factory agent bound to the repository."""

    with TestClient(create_app()) as client:
        if new_agent:
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
        yield Factory(client, github)


@pytest.fixture
def factory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Factory]:
    github = factory_github(monkeypatch, tmp_path)
    try:
        with running_factory(github, new_agent=True) as running:
            yield running
    finally:
        get_settings.cache_clear()


# --- criterion 1: plain feedback admits one revision ------------------------------------


@pytest.mark.parametrize("path", PATHS)
def test_plain_conversation_comment_admits_one_revision(factory: Factory, path: str) -> None:
    pull = factory.pull()
    posted = factory.comment(pull, "Please rename the helper to parse_fields.")

    responses = factory.deliver(path, posted)

    assert [r.json()["status"] for r in responses] == (["factory_admitted"] if responses else [])
    rows = _requests(pull.issue)
    assert len(rows) == 2
    revision = rows[1]
    assert revision["work_item_id"] == pull.work_item_id
    comment_id = posted.item["id"]
    assert revision["id"] == uuid.uuid5(
        uuid.NAMESPACE_URL,
        "github-feedback-"
        + str(uuid.uuid5(uuid.NAMESPACE_URL, f"{REPO_ID}:issue_comment:{comment_id}")),
    )
    assert revision["reply_target_kind"] == "pull_request"
    assert (
        revision["reply_target_url"]
        == f"{HTML}/{REPO}/pull/{pull.number}#issuecomment-{comment_id}"
    )
    assert _lineage(pull)["plain_feedback_revisions"] == 1


@pytest.mark.parametrize("path", PATHS)
def test_plain_inline_comment_of_a_submitted_review_admits_one_revision(
    factory: Factory, path: str
) -> None:
    """The inline comment is written in a pending review; submission, 8 s later,
    bumps its ``updated_at`` without an edit (#2794). That bump is no edit. On
    the poll path the review listing fails for the pass, so only the inline
    comment listing can admit it."""

    pull = factory.pull()
    pending = factory.github.create_review(
        pull.number,
        REVIEWER,
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    review_id = pending.item["id"]
    factory.github.tick(8)
    submitted = factory.github.submit_review(pull.number, review_id, "COMMENT")
    (inline,) = factory.github.comments_of_review(review_id)
    assert inline["updated_at"] != inline["created_at"]

    if path == "webhook":
        factory.github.drain_deliveries()
        review_delivery, comment_delivery = submitted.deliveries
        first = factory.post(comment_delivery)
        assert first.json()["status"] == "factory_admitted", first.text
        assert factory.post(review_delivery).json()["status"] == "factory_duplicate"
    else:
        factory.github.drain_deliveries()
        reviews = f"/repos/{REPO}/pulls/{pull.number}/reviews"
        factory.github.fail_reads(f"GET {reviews}$", 502)
        factory.poll()
        assert reviews in {request.url.path for request in factory.github.http_requests}

    rows = _requests(pull.issue)
    assert len(rows) == 2
    assert rows[1]["id"] == _review_key(pull.number, review_id)
    assert rows[1]["work_item_id"] == pull.work_item_id
    provenance = _provenance(rows[1]["objective"])
    assert provenance["comments"] == [
        {"path": "src/acme/helper.py", "line": 12, "body": "Rename this helper."}
    ]
    assert _lineage(pull)["plain_feedback_revisions"] == 1


@pytest.mark.parametrize("path", PATHS)
def test_plain_review_with_text_admits_one_revision(factory: Factory, path: str) -> None:
    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number, REVIEWER, body="Split the parser into two functions.", event="REQUEST_CHANGES"
    )

    factory.deliver(path, posted)

    rows = _requests(pull.issue)
    assert len(rows) == 2
    assert rows[1]["id"] == _review_key(pull.number, posted.item["id"])
    assert rows[1]["work_item_id"] == pull.work_item_id
    provenance = _provenance(rows[1]["objective"])
    assert provenance["body"] == "Split the parser into two functions."
    assert provenance["state"] == "CHANGES_REQUESTED"
    assert provenance["comments"] == []


# --- criterion 2: refusals, and the mention liveness of approved reviews ----------------


def _assert_refused(
    factory: Factory, pull: Pull, path: str, posted: Posted, codes: list[str]
) -> None:
    before = _all_requests()
    responses = factory.deliver(path, posted)
    for response in responses:
        assert response.json()["status"] == "factory_ignored", response.text
    if path == "webhook":
        assert _codes(responses) == codes
    assert _all_requests() == before
    assert len(_requests(pull.issue)) == 1
    assert _lineage(pull)["plain_feedback_revisions"] == 0


@pytest.mark.parametrize("path", PATHS)
def test_read_permission_sender_is_refused(factory: Factory, path: str) -> None:
    factory.github.grant(READER, "read")
    pull = factory.pull()
    posted = factory.comment(pull, "Please rename the helper.", READER)
    _assert_refused(factory, pull, path, posted, ["sender_permission_refused"])


@pytest.mark.parametrize("path", PATHS)
def test_bot_sender_is_refused(factory: Factory, path: str) -> None:
    pull = factory.pull()
    posted = factory.comment(pull, "Please rename the helper.", BOT)
    _assert_refused(factory, pull, path, posted, ["non_human_sender"])


@pytest.mark.parametrize("path", PATHS)
def test_factory_app_comment_is_refused(factory: Factory, path: str) -> None:
    pull = factory.pull()
    posted = factory.comment(pull, "Please rename the helper.")
    via = {"id": int(APP_ID), "slug": "curie-fixture"}
    stored = next(c for c in factory.github.comments if c["id"] == posted.item["id"])
    stored["performed_via_github_app"] = via
    payload = posted.deliveries[0].payload
    payload["comment"]["performed_via_github_app"] = via
    app_posted = Posted(stored, (signed_delivery("issue_comment", payload),))
    _assert_refused(factory, pull, path, app_posted, ["app_authored"])


@pytest.mark.parametrize("path", PATHS)
def test_edited_conversation_comment_is_refused(factory: Factory, path: str) -> None:
    pull = factory.pull()
    posted = factory.comment(pull, "Please rename the helper.")
    factory.github.tick(3)
    factory.github.edit_comment(posted.item["id"], "Please rename the helper and the module.")
    _assert_refused(factory, pull, path, posted, ["edited_feedback"])


@pytest.mark.parametrize("path", PATHS)
def test_inline_comment_edited_after_submission_is_refused(factory: Factory, path: str) -> None:
    """Edited 10 s after submission, past the 5 s submission allowance. The
    review event arrives after the edit and is refused for it."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    (inline,) = factory.github.comments_of_review(posted.item["id"])
    factory.github.tick(10)
    factory.github.edit_comment(inline["id"], "Rename this helper and its callers.")
    review_only = Posted(posted.item, posted.deliveries[:1])
    _assert_refused(factory, pull, path, review_only, ["edited_feedback"])
    if path == "webhook":
        # The inline comment's own delivery no longer matches GitHub's copy.
        late = factory.post(posted.deliveries[1])
        assert late.json()["status"] == "factory_ignored", late.text
        assert len(_requests(pull.issue)) == 1


@pytest.mark.parametrize("path", PATHS)
def test_an_edited_sibling_never_loses_the_mention_beside_it(factory: Factory, path: str) -> None:
    """A writer submits a review of two inline comments, one mentioning the
    factory, then edits only the other 60 s later, before intake. The edited
    comment is left out of the review's feedback; the untouched mention is
    still admitted (decision 2.3), on the mention's original signed delivery
    and on the poll alike, and is not counted as plain feedback."""

    pull = factory.pull()
    mention = f"@{MENTION} please rename this helper."
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        comments=[
            _comment("src/acme/helper.py", 12, mention),
            _comment("src/acme/helper.py", 30, "Also tidy this loop."),
        ],
    )
    review_id = posted.item["id"]
    asked, sibling = factory.github.comments_of_review(review_id)
    assert asked["body"] == mention
    factory.github.tick(60)
    factory.github.edit_comment(sibling["id"], "Also tidy this loop and its guard.")
    factory.github.drain_deliveries()

    if path == "webhook":
        (asked_delivery,) = [
            delivery
            for delivery in posted.deliveries
            if delivery.payload.get("comment", {}).get("id") == asked["id"]
        ]
        response = factory.post(asked_delivery)
        assert response.json()["status"] == "factory_admitted", response.text
        # The edited sibling's own delivery starts nothing more.
        (sibling_delivery,) = [
            delivery
            for delivery in posted.deliveries
            if delivery.payload.get("comment", {}).get("id") == sibling["id"]
        ]
        assert factory.post(sibling_delivery).json()["status"] != "factory_admitted"
    else:
        factory.poll()

    rows = _requests(pull.issue)
    assert len(rows) == 2
    assert rows[1]["id"] == _review_key(pull.number, review_id)
    assert _provenance(rows[1]["objective"])["comments"] == [
        {"path": "src/acme/helper.py", "line": 12, "body": mention}
    ]
    assert "tidy this loop" not in rows[1]["objective"]
    assert _lineage(pull)["plain_feedback_revisions"] == 0


@pytest.mark.parametrize("path", PATHS)
def test_an_edited_inline_comment_is_still_refused_when_it_arrives_itself(
    factory: Factory, path: str
) -> None:
    """The counterpart: the edited comment of a review with no other feedback
    is refused, whether its own delivery (whose text no longer matches
    GitHub's copy) or the poll brings it."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="",
        event="COMMENT",
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    (inline,) = factory.github.comments_of_review(posted.item["id"])
    factory.github.tick(60)
    factory.github.edit_comment(inline["id"], "Rename this helper and its callers.")
    comment_only = Posted(
        posted.item,
        tuple(d for d in posted.deliveries if d.payload.get("comment", {}).get("id")),
    )
    _assert_refused(factory, pull, path, comment_only, ["feedback_changed"])


@pytest.mark.parametrize("path", PATHS)
def test_empty_review_without_inline_comments_is_refused(factory: Factory, path: str) -> None:
    pull = factory.pull()
    posted = factory.github.create_review(pull.number, REVIEWER, body="", event="COMMENT")
    _assert_refused(factory, pull, path, posted, ["empty_feedback"])


@pytest.mark.parametrize("path", PATHS)
def test_plain_pieces_of_an_approved_review_are_refused(factory: Factory, path: str) -> None:
    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="Looks good, one nit.",
        event="APPROVE",
        comments=[_comment("src/acme/helper.py", 12, "Nit: rename this helper.")],
    )
    _assert_refused(factory, pull, path, posted, ["non_actionable_review", "non_actionable_review"])


@pytest.mark.parametrize("path", PATHS)
def test_inline_comment_on_a_commit_other_than_the_head_is_refused(
    factory: Factory, path: str
) -> None:
    pull = factory.pull()
    older = factory.github.branches["main"]
    assert older != factory.head(pull)
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        commit_id=older,
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    _assert_refused(factory, pull, path, posted, ["stale_feedback_head", "stale_feedback_head"])


@pytest.mark.parametrize("path", PATHS)
def test_feedback_on_a_closed_lineage_is_refused(factory: Factory, path: str) -> None:
    pull = factory.pull(lineage_status="closed")
    posted = factory.comment(pull, "Please rename the helper.")
    _assert_refused(factory, pull, path, posted, ["lineage_closed"])


@pytest.mark.parametrize("path", PATHS)
def test_feedback_on_a_cancelled_work_item_is_refused(factory: Factory, path: str) -> None:
    pull = factory.pull()
    _execute(
        "UPDATE curie.work_items SET cancelled_at = clock_timestamp(), "
        "version = version + 1 WHERE id = :id",
        {"id": pull.work_item_id},
    )
    posted = factory.comment(pull, "Please rename the helper.")
    _assert_refused(factory, pull, path, posted, ["work_item_cancelled"])


@pytest.mark.parametrize("mentions", [1, 2])
@pytest.mark.parametrize("path", PATHS)
def test_mentions_in_an_approved_review_are_not_refused(
    factory: Factory, path: str, mentions: int
) -> None:
    """Liveness of the approved-review refusal: each inline comment that
    mentions the factory admits on its own, as before #5794 (resolution 8)."""

    pull = factory.pull()
    bodies = [f"@{MENTION} please rename helper {index}." for index in range(mentions)]
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="Approved once these are renamed.",
        event="APPROVE",
        comments=[_comment("src/acme/helper.py", 10 + i, body) for i, body in enumerate(bodies)],
    )
    inline_ids = [c["id"] for c in factory.github.comments_of_review(posted.item["id"])]

    responses = factory.deliver(path, posted)

    if path == "webhook":
        assert _codes(responses[:1]) == ["non_actionable_review"]
    rows = _requests(pull.issue)
    assert len(rows) == 1 + mentions
    expected = {
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "github-feedback-"
            + str(uuid.uuid5(uuid.NAMESPACE_URL, f"{REPO_ID}:pull_request_review_comment:{cid}")),
        )
        for cid in inline_ids
    }
    assert {row["id"] for row in rows[1:]} == expected
    assert {row["reply_target_kind"] for row in rows[1:]} == {"review_thread"}
    # A mention is never counted against the plain cap.
    assert _lineage(pull)["plain_feedback_revisions"] == 0


def test_approved_review_inline_comment_edited_to_add_a_mention_is_refused(
    factory: Factory,
) -> None:
    """An approved review's plain inline comment edited 10 s after submission
    to add the mention, past the 5 s submission allowance. Its webhook was
    plain; the poll lists the edited comment, whose fresh read matches, and
    admits nothing."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="Approved.",
        event="APPROVE",
        comments=[_comment("src/acme/helper.py", 12, "Nit: rename this helper.")],
    )
    (inline,) = factory.github.comments_of_review(posted.item["id"])
    factory.github.tick(10)
    factory.github.edit_comment(inline["id"], f"@{MENTION} nit: rename this helper.")
    factory.github.drain_deliveries()
    before = _all_requests()

    factory.poll()

    assert _all_requests() == before
    assert len(_requests(pull.issue)) == 1
    listing = f"/repos/{REPO}/pulls/{pull.number}/reviews/{posted.item['id']}/comments"
    assert listing in {request.url.path for request in factory.github.http_requests}


@pytest.mark.parametrize("listings", ["both", "reviews_only"])
def test_upgrade_review_answered_under_a_per_event_id_is_not_answered_again(
    factory: Factory, listings: str
) -> None:
    """Before #5794 a mentioning inline comment was admitted under its own
    per-event id. The row is written by the production admission: today's
    approved-review mention path is the base's per-piece admission unchanged
    (same request id, objective and review-thread reply target). GitHub then
    reports the review as commented, as for a base-era review answered that
    way. On the unchanged head a poll pass that lists the review again, alone
    (``reviews_only``: the inline comment listing is not modified) or with its
    inline comment, admits nothing."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="One rename.",
        event="APPROVE",
        comments=[_comment("src/acme/helper.py", 12, f"@{MENTION} please rename this helper.")],
    )
    factory.github.drain_deliveries()
    _, inline = posted.deliveries
    assert factory.post(inline).json()["status"] in {"factory_admitted", "factory_queued"}
    rows = _requests(pull.issue)
    assert len(rows) == 2
    per_event = uuid.uuid5(
        uuid.NAMESPACE_URL,
        "github-feedback-"
        + str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{REPO_ID}:pull_request_review_comment:{inline.payload['comment']['id']}",
            )
        ),
    )
    assert (rows[1]["id"], rows[1]["reply_target_kind"]) == (per_event, "review_thread")
    factory.github._review_of(pull.number, posted.item["id"])["state"] = "COMMENTED"
    if listings == "reviews_only":
        factory.github.fail_reads(rf"GET /repos/{REPO}/pulls/comments$", 304)

    factory.poll()

    review_comments = f"/repos/{REPO}/pulls/{pull.number}/reviews/{posted.item['id']}/comments"
    assert review_comments in {request.url.path for request in factory.github.http_requests}
    assert [row["id"] for row in _requests(pull.issue)] == [row["id"] for row in rows]
    assert _lineage(pull)["plain_feedback_revisions"] == 0


# --- criterion 3: one submitted review coalesces into one request -------------------------


@pytest.mark.parametrize("order", ["review_first", "comments_first", "interleaved"])
def test_review_pieces_coalesce_into_one_request(factory: Factory, order: str) -> None:
    pull = factory.pull()
    texts = {
        "src/acme/parser.py": "Split this function.",
        "src/acme/helper.py": "Rename this helper.",
        "README.md": "Document the new flag.",
    }
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="Three changes before merge.",
        event="REQUEST_CHANGES",
        comments=[
            _comment(path, 7 if path != "README.md" else None, body) for path, body in texts.items()
        ],
    )
    factory.github.drain_deliveries()
    review, *comments = posted.deliveries
    sequence = {
        "review_first": [review, *comments],
        "comments_first": [*comments, review],
        "interleaved": [comments[0], review, comments[2], comments[1]],
    }[order]

    statuses = [factory.post(delivery).json()["status"] for delivery in sequence]
    factory.poll()

    assert statuses == ["factory_admitted"] + ["factory_duplicate"] * 3
    rows = _requests(pull.issue)
    assert len(rows) == 2
    revision = rows[1]
    assert revision["id"] == _review_key(pull.number, posted.item["id"])
    assert revision["reply_target_kind"] == "pull_request"
    assert revision["reply_target_comment_id"] is None
    provenance = _provenance(revision["objective"])
    assert provenance["body"] == "Three changes before merge."
    assert {(c["path"], c["body"]) for c in provenance["comments"]} == set(texts.items())
    assert {c["path"]: c["line"] for c in provenance["comments"]}["README.md"] is None
    assert _lineage(pull)["plain_feedback_revisions"] == 1


# --- criterion 4: the poll sees an inline comment before its review -----------------------


def test_inline_before_review_pending_review_creates_nothing_until_submitted(
    factory: Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(a) The review is pending: the App's review read is a 404, so a poll pass
    creates nothing. GitHub's listings omit a pending comment; here the inline
    comment leaks into the App's comment listing anyway, so the poll's
    admission reaches the review read and is refused there. After submission
    with a second inline comment, the next poll and the review event together
    create one request with the body and both comments."""

    pull = factory.pull()
    pending = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="Two renames.",
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    review_id = pending.item["id"]
    author_only = factory.github._sees

    def leaking(viewer: dict[str, Any], item: dict[str, Any]) -> bool:
        # Comments leak to every viewer; the review itself stays its author's.
        return "pull_request_review_id" in item or author_only(viewer, item)

    monkeypatch.setattr(factory.github, "_sees", leaking)
    review_read = f"/repos/{REPO}/pulls/{pull.number}/reviews/{review_id}"
    factory.poll()
    assert review_read in {request.url.path for request in factory.github.http_requests}
    assert len(_requests(pull.issue)) == 1

    factory.github._add_review_comment(
        pull.number,
        review_id,
        REVIEWER,
        _comment("src/acme/parser.py", 30, "Rename this parser."),
        factory.head(pull),
        factory.github._now(),
    )
    factory.github.tick(4)
    submitted = factory.github.submit_review(pull.number, review_id, "COMMENT")
    factory.github.drain_deliveries()
    factory.poll()
    replay = factory.post(submitted.deliveries[0])

    assert replay.json()["status"] == "factory_duplicate", replay.text
    rows = _requests(pull.issue)
    assert len(rows) == 2
    assert rows[1]["id"] == _review_key(pull.number, review_id)
    provenance = _provenance(rows[1]["objective"])
    assert provenance["body"] == "Two renames."
    assert [c["body"] for c in provenance["comments"]] == [
        "Rename this helper.",
        "Rename this parser.",
    ]


def test_inline_before_review_listing_unread_takes_the_whole_review(factory: Factory) -> None:
    """(b) The review is submitted, but the pass has not read the review listing
    (it fails after the inline comments are admitted). The inline comment's
    poll admits one request holding every comment of the review; the review
    event later replays it."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="Two renames.",
        event="COMMENT",
        comments=[
            _comment("src/acme/helper.py", 12, "Rename this helper."),
            _comment("src/acme/parser.py", 30, "Rename this parser."),
        ],
    )
    factory.github.drain_deliveries()
    factory.github.fail_reads(rf"GET /repos/{REPO}/pulls/{pull.number}/reviews$", 502)
    factory.poll()

    assert any(
        request.url.path == f"/repos/{REPO}/pulls/{pull.number}/reviews"
        for request in factory.github.http_requests
    )
    rows = _requests(pull.issue)
    assert len(rows) == 2
    assert rows[1]["id"] == _review_key(pull.number, posted.item["id"])
    provenance = _provenance(rows[1]["objective"])
    assert provenance["body"] == "Two renames."
    assert {c["body"] for c in provenance["comments"]} == {
        "Rename this helper.",
        "Rename this parser.",
    }
    replay = factory.post(posted.deliveries[0])
    assert replay.json()["status"] == "factory_duplicate", replay.text
    assert len(_requests(pull.issue)) == 2


def test_inline_before_review_redelivery_after_submission_is_decided_again(
    factory: Factory,
) -> None:
    """Resolution 4. An inline comment delivered while its review is pending is
    refused ``review_not_submitted`` with its receipt left retryable, so the
    same delivery id redelivered after submission admits the review."""

    pull = factory.pull()
    pending = factory.github.create_review(
        pull.number,
        REVIEWER,
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    (inline,) = factory.github.comments_of_review(pending.item["id"])
    visible = {k: v for k, v in inline.items() if not k.startswith("_") or k == "_links"}
    early = signed_delivery(
        "pull_request_review_comment",
        factory.github._webhook("created", pull.number, REVIEWER, comment=visible),
    )

    refused = factory.post(early)
    assert (refused.json()["status"], _code(refused)) == (
        "factory_ignored",
        "review_not_submitted",
    )
    assert _rows(
        "SELECT status FROM curie.github_review_deliveries WHERE delivery_id = :id",
        {"id": uuid.UUID(early.delivery_id)},
    ) == [{"status": "retryable"}]
    assert len(_requests(pull.issue)) == 1

    factory.github.submit_review(pull.number, pending.item["id"], "COMMENT")
    factory.github.drain_deliveries()
    again = factory.post(early)

    assert again.json()["status"] == "factory_admitted", again.text
    rows = _requests(pull.issue)
    assert len(rows) == 2
    assert rows[1]["id"] == _review_key(pull.number, pending.item["id"])


# --- criterion 5: real review shapes, end to end --------------------------------------------


@pytest.mark.parametrize("delivery", ["webhook_review_first", "webhook_comments_first", "poll"])
@pytest.mark.parametrize("case", ["COMMENT", "REQUEST_CHANGES"])
def test_real_review_shapes_admit_the_submitted_batch_once(
    factory: Factory, case: str, delivery: str
) -> None:
    """A writer creates the review through the REST reviews API with their own
    token: pending then submitted with COMMENT, or REQUEST_CHANGES given in the
    creating call. Each delivery order runs on a fresh database."""

    pull = factory.pull()
    comments = [
        _comment("src/acme/helper.py", 12, "Rename this helper."),
        _comment("src/acme/parser.py", 30, "Split this parser."),
    ]
    token = factory.github.user_token(REVIEWER)
    with factory.github.client_for(token) as reviewer:
        factory.poll()
        created = reviewer.post(
            f"/repos/{REPO}/pulls/{pull.number}/reviews",
            json={"body": "Two changes, please.", "comments": comments}
            | ({"event": case} if case == "REQUEST_CHANGES" else {}),
        )
        assert created.status_code == 200, created.text
        review_id = created.json()["id"]
        if case == "COMMENT":
            assert created.json()["state"] == "PENDING"
            factory.poll()
            assert len(_requests(pull.issue)) == 1
            submitted = reviewer.post(
                f"/repos/{REPO}/pulls/{pull.number}/reviews/{review_id}/events",
                json={"event": "COMMENT"},
            )
            assert submitted.status_code == 200, submitted.text
    deliveries = factory.github.drain_deliveries()
    review, *inline = deliveries
    assert [d.event for d in deliveries] == ["pull_request_review"] + [
        "pull_request_review_comment"
    ] * 2
    assert {d.payload["comment"]["pull_request_review_id"] for d in inline} == {review_id}

    if delivery == "webhook_review_first":
        statuses = [factory.post(d).json()["status"] for d in [review, *inline]]
    elif delivery == "webhook_comments_first":
        statuses = [factory.post(d).json()["status"] for d in [*inline, review]]
    else:
        factory.poll()
        statuses = []
    if statuses:
        assert sorted(statuses) == ["factory_admitted"] + ["factory_duplicate"] * 2

    rows = _requests(pull.issue)
    assert len(rows) == 2
    revision = rows[1]
    assert revision["id"] == _review_key(pull.number, review_id)
    assert revision["work_item_id"] == pull.work_item_id
    assert revision["reply_target_kind"] == "pull_request"
    assert revision["reply_target_url"] == _review_url(pull.number, review_id)
    provenance = _provenance(revision["objective"])
    assert revision["objective"].splitlines()[0] == _review_url(pull.number, review_id)
    assert provenance["body"] == "Two changes, please."
    assert provenance["comments"] == [
        {"path": "src/acme/helper.py", "line": 12, "body": "Rename this helper."},
        {"path": "src/acme/parser.py", "line": 30, "body": "Split this parser."},
    ]

    for again in deliveries:
        assert factory.post(again).json()["status"] in {"factory_duplicate"}
        redelivered = signed_delivery(again.event, again.payload)
        assert factory.post(redelivered).json()["status"] == "factory_duplicate"
    factory.poll()
    assert [row["id"] for row in _requests(pull.issue)] == [row["id"] for row in rows]


# --- criteria 6 and 7: the plain feedback cap ------------------------------------------------


def _plain(factory: Factory, pull: Pull, path: str, index: int) -> list[httpx.Response]:
    return factory.deliver(path, factory.comment(pull, f"Please also rename helper {index}."))


@pytest.mark.parametrize("path", PATHS)
def test_cap_after_ten_plain_revisions_needs_a_mention(factory: Factory, path: str) -> None:
    pull = factory.pull()
    before = _lineage(pull)

    _plain(factory, pull, path, 1)
    after_one = _lineage(pull)
    assert after_one["plain_feedback_revisions"] == 1
    # Counting never moves the lineage's own updated_at or version.
    assert (after_one["updated_at"], after_one["version"]) == (
        before["updated_at"],
        before["version"],
    )
    for index in range(2, 10):
        _plain(factory, pull, path, index)
    assert len(_requests(pull.issue)) == 10

    # The tenth plain revision is one review: its later piece replays, even
    # though the count reached the cap with the review's first piece.
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        body="One more rename.",
        event="COMMENT",
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    factory.github.drain_deliveries()
    if path == "webhook":
        first, later = posted.deliveries
        assert factory.post(first).json()["status"] in {"factory_admitted", "factory_queued"}
        assert _lineage(pull)["plain_feedback_revisions"] == 10
        assert factory.post(later).json()["status"] == "factory_duplicate"
    else:
        factory.poll()
    assert len(_requests(pull.issue)) == 11
    assert _lineage(pull)["plain_feedback_revisions"] == 10
    assert factory.notices(pull) == []

    eleventh = _plain(factory, pull, path, 11)
    if path == "webhook":
        assert _codes(eleventh) == ["plain_feedback_cap"]
    assert len(_requests(pull.issue)) == 11
    (notice,) = factory.notices(pull)
    assert NOTICE in notice["body"]
    assert f"{NOTICE_MARKER}{pull.lineage_id}" in notice["body"]
    assert notice["user"]["type"] == "Bot"
    assert notice["performed_via_github_app"] is not None
    assert _lineage(pull)["plain_feedback_capped_at"] is not None

    twelfth = _plain(factory, pull, path, 12)
    if path == "webhook":
        assert _codes(twelfth) == ["plain_feedback_cap"]
    assert len(_requests(pull.issue)) == 11
    assert len(factory.notices(pull)) == 1

    factory.deliver(path, factory.comment(pull, f"@{MENTION} please rename one more helper."))
    assert len(_requests(pull.issue)) == 12
    assert _lineage(pull)["plain_feedback_revisions"] == 10
    assert len(factory.notices(pull)) == 1


def test_cap_notice_that_fails_to_post_blocks_no_later_feedback(factory: Factory) -> None:
    """The mention-needed notice post is refused once (403). The 11th plain
    comment is refused with no notice and the cap left unrecorded; a mention
    later in the same pass is admitted and the cursor moves past both. A 12th
    plain comment on a later pass posts the notice once."""

    pull = factory.pull()
    for index in range(1, 11):
        _plain(factory, pull, "poll", index)
    assert len(_requests(pull.issue)) == 11
    notice_post = f"/repos/{REPO}/issues/{pull.number}/comments"
    factory.github.fail_writes(f"POST {notice_post}$", 403)
    factory.comment(pull, "Please also rename helper 11.")
    mention = factory.comment(pull, f"@{MENTION} please rename one more helper.")
    factory.github.drain_deliveries()

    factory.poll()

    assert any(
        request.method == "POST" and request.url.path == notice_post
        for request in factory.github.http_requests
    )
    assert factory.notices(pull) == []
    rows = _requests(pull.issue)
    assert len(rows) == 12
    assert MENTION in _provenance(rows[-1]["objective"])["body"]
    lineage = _lineage(pull)
    assert (lineage["plain_feedback_revisions"], lineage["plain_feedback_capped_at"]) == (10, None)
    (cursor,) = _rows("SELECT comments_since FROM curie.factory_poll_cursors")
    assert cursor["comments_since"] == datetime.fromisoformat(
        mention.item["created_at"].replace("Z", "+00:00")
    )

    _plain(factory, pull, "poll", 12)

    assert len(_requests(pull.issue)) == 12
    (notice,) = factory.notices(pull)
    assert NOTICE in notice["body"]
    assert _lineage(pull)["plain_feedback_capped_at"] is not None


def test_per_pull_request_count_starts_at_zero(factory: Factory) -> None:
    first = factory.pull()
    for index in range(1, 11):
        _plain(factory, first, "webhook", index)
    assert _codes(_plain(factory, first, "webhook", 11)) == ["plain_feedback_cap"]
    assert len(factory.notices(first)) == 1

    second = factory.pull()
    assert second.work_item_id != first.work_item_id
    admitted = _plain(factory, second, "webhook", 1)

    assert [r.json()["status"] for r in admitted] == ["factory_admitted"]
    assert len(_requests(second.issue)) == 2
    assert _lineage(second)["plain_feedback_revisions"] == 1
    assert factory.notices(second) == []
    assert _lineage(first)["plain_feedback_revisions"] == 10


# --- handoff proofs --------------------------------------------------------------------------


def test_handoff_signed_bytes_reach_the_route_and_a_tampered_body_does_not(
    factory: Factory,
) -> None:
    """Counterexample: the same delivery with one changed body byte and the
    original signature is rejected before anything is read or admitted."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number, REVIEWER, body="Rename the helper.", event="COMMENT"
    )
    factory.github.drain_deliveries()
    (delivery,) = posted.deliveries
    tampered = WebhookDelivery(
        delivery.event,
        {**delivery.headers, "X-GitHub-Delivery": str(uuid.uuid4())},
        delivery.body.replace(b"Rename the helper.", b"Rename the helpers"),
    )

    rejected = post_delivery(factory.client, tampered)
    assert rejected.status_code == 401, rejected.text
    assert len(_requests(pull.issue)) == 1
    assert factory.post(delivery).json()["status"] == "factory_admitted"
    assert len(_requests(pull.issue)) == 2


@pytest.mark.parametrize("other", ["pending", "other_author"])
def test_handoff_inline_comment_naming_another_review_creates_nothing(
    factory: Factory, other: str
) -> None:
    """Counterexample: an inline comment delivery whose pull_request_review_id
    names another review, a pending one or one by another author."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    foreign = factory.github.create_review(
        pull.number,
        SECOND,
        body="Second opinion.",
        event=None if other == "pending" else "COMMENT",
    )
    factory.github.drain_deliveries()
    payload = posted.deliveries[1].payload
    payload["comment"]["pull_request_review_id"] = foreign.item["id"]

    response = factory.post(signed_delivery("pull_request_review_comment", payload))

    assert response.json()["status"] == "factory_ignored", response.text
    assert _code(response) == (
        "review_not_submitted" if other == "pending" else "feedback_target_mismatch"
    )
    assert len(_requests(pull.issue)) == 1


def test_handoff_webhook_review_state_is_never_the_authority(factory: Factory) -> None:
    """The review's REST read decides. Counterexamples: an upper case webhook
    state for a commented review still admits; an approved review whose
    delivery claims ``commented`` admits nothing."""

    pull = factory.pull()
    approved = factory.github.create_review(
        pull.number, REVIEWER, body="Approved with a nit.", event="APPROVE"
    )
    forged = approved.deliveries[0].payload
    forged["review"]["state"] = "commented"
    factory.github.drain_deliveries()
    refused = factory.post(signed_delivery("pull_request_review", forged))
    assert (refused.json()["status"], _code(refused)) == (
        "factory_ignored",
        "non_actionable_review",
    )
    assert len(_requests(pull.issue)) == 1

    commented = factory.github.create_review(
        pull.number, REVIEWER, body="Rename the helper.", event="COMMENT"
    )
    factory.github.drain_deliveries()
    upper = commented.deliveries[0].payload
    upper["review"]["state"] = "COMMENTED"
    admitted = factory.post(signed_delivery("pull_request_review", upper))
    assert admitted.json()["status"] == "factory_admitted", admitted.text
    assert len(_requests(pull.issue)) == 2


def test_handoff_comments_of_another_review_never_enter_the_objective(
    factory: Factory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two reviews by one author: each request holds only its own comments.
    Counterexample: a review comment listing that also returns another
    review's comment admits nothing."""

    pull = factory.pull()
    first = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        comments=[_comment("src/acme/helper.py", 12, "First review comment.")],
    )
    second = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        comments=[_comment("src/acme/parser.py", 30, "Second review comment.")],
    )
    factory.github.drain_deliveries()
    for posted in (first, second):
        assert factory.post(posted.deliveries[0]).json()["status"] in {
            "factory_admitted",
            "factory_queued",
        }
    rows = _requests(pull.issue)
    assert len(rows) == 3
    by_id = {row["id"]: _provenance(row["objective"]) for row in rows[1:]}
    assert [c["body"] for c in by_id[_review_key(pull.number, first.item["id"])]["comments"]] == [
        "First review comment."
    ]
    assert [c["body"] for c in by_id[_review_key(pull.number, second.item["id"])]["comments"]] == [
        "Second review comment."
    ]

    third = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        comments=[_comment("src/acme/cli.py", 5, "Third review comment.")],
    )
    factory.github.drain_deliveries()
    listing = factory.github.comments_of_review
    leaked = factory.github.comments_of_review(first.item["id"])

    def with_leak(review_id: int) -> list[dict[str, Any]]:
        own = listing(review_id)
        return own + leaked if review_id == third.item["id"] else own

    monkeypatch.setattr(factory.github, "comments_of_review", with_leak)
    response = factory.post(third.deliveries[0])
    assert (response.json()["status"], _code(response)) == (
        "factory_ignored",
        "feedback_target_mismatch",
    )
    assert len(_requests(pull.issue)) == 3


def test_handoff_review_url_is_what_the_status_comment_answers(factory: Factory) -> None:
    """The stored reply_target_url is read back by the status comment sync: the
    request's terminal comment on the pull request conversation says "In
    response to" the review URL. Counterexample: the inline comment's own
    ``discussion_r`` URL, which delivered first, appears nowhere."""

    pull = factory.pull()
    posted = factory.github.create_review(
        pull.number,
        REVIEWER,
        event="COMMENT",
        comments=[_comment("src/acme/helper.py", 12, "Rename this helper.")],
    )
    factory.github.drain_deliveries()
    review, inline = posted.deliveries
    assert factory.post(inline).json()["status"] == "factory_admitted"
    discussion = inline.payload["comment"]["html_url"]
    assert "#discussion_r" in discussion

    factory.github.label_event(pull.issue, "unlabeled", LABEL, dict(REVIEWER))
    cancelled = _post(
        factory.client, "issues", _issue_event("unlabeled", pull.issue, label={"name": LABEL})
    )
    assert cancelled.json()["status"] == "factory_cancelled", cancelled.text
    factory.reconcile()

    answered = [
        comment["body"]
        for comment in factory.github.comments
        if comment["_number"] == pull.number and "In response to" in comment["body"]
    ]
    assert len(answered) == 1, [c["body"] for c in factory.github.comments]
    assert (
        f"In response to {_review_url(pull.number, review.payload['review']['id'])}"
        in (answered[0])
    )
    assert discussion not in answered[0]
