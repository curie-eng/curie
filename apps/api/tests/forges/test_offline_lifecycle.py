"""The whole factory lifecycle, offline, for the github pairing (#4398).

One lifecycle runs for the github adapter pair (ADR 0197, "Two ports"): a ticket is
marked, admission creates the WorkItem, the reconciler publishes the execute
wake, a worker starts the run, a publication opens a pull request, CI on its
head is red and the CI gate asks for a fix round on the same request, the fix
head is green and the request completes, review feedback admits a revision
that completes in turn, and every step's status reply and state label lands on
the side its reply target names. Persisted state is asserted at every step.

The ``github`` pair drives the GitHub fakes in ``forge_fakes``: the label and
the review feedback arrive as signed webhooks on ``/github/webhook``, and the
CI gate, status comments and labels talk HTTP to the threaded GitHub REST fake.

The helpers insert publications and fix rounds directly (the approval and
publication Job path is not offline), with the helpers the factory terminus
and CI wait suites use. Placeholder identifiers only.
"""

# ruff: noqa: F811  (the shared ``comments`` fixture is imported, then requested)

from __future__ import annotations

import copy
import json
import re
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
import pytest
import redis
from aci_protocol import STREAM_PAYLOAD_FIELD
from channel_protocol.work_item_events import execute_event_id
from curie_api import factory_notices
from curie_api.config import get_settings
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import FINAL_MARKER
from curie_api.forges import types
from curie_api.main import create_app
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from forge_fakes import factory_wake
from forge_fakes.github import (
    INSTALLATION_ID,
    LABEL,
    MENTION,
    REPO,
    REPO_ID,
    GitHubAPI,
    _issue_event,
    _post,
    _sender,
)
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    HEAD_A,
    HEAD_B,
    HEAD_C,
    _clear_ci_keys,
    _CommentServer,
    _rows,
    ci_failing,
    ci_green,
    ci_pending,
    comments,
)
from sqlalchemy import text
from test_factory_ci_wait import _attach_fix
from test_factory_terminus import (
    _attach_publication,
    _reconcile,
    _set_base_ref,
)

pytestmark = pytest.mark.usefixtures("clean_db")

STATE_LABELS = frozenset(factory_notices.STATE_LABELS)
REVIEW_BODY = f"@{MENTION} please rename the helper before merge"
ISSUE_NUMBER = 9481
PR_NUMBER = 731
FEEDBACK_ID = 88131


@dataclass(frozen=True)
class Write:
    """One status reply write: a create or an edit, on a reply target kind."""

    op: str
    target: str
    body: str


class Pair(Protocol):
    """What one adapter pairing supplies to the shared lifecycle."""

    client: TestClient
    tracker_kind: str
    tracker_issue_id: str
    # Whether the status sink records the posted reply on its notice row, so a
    # final reply is frozen. Only the GitHub sink does today.
    finalizes: bool

    @property
    def issue_url(self) -> str: ...

    def mark(self) -> uuid.UUID: ...

    def open_pull_request(self) -> tuple[int, str]: ...

    def push_head(self, stage: str) -> str: ...

    def set_ci(self, sha: str, *, green: bool) -> None: ...

    def review(self, work_item_id: uuid.UUID, pr: int) -> uuid.UUID: ...

    def writes(self, request_id: uuid.UUID) -> list[Write]: ...

    def state_labels(self) -> set[str]: ...

    def assert_terminal(self, first: uuid.UUID, revision: uuid.UUID) -> None: ...


# --- persisted state -------------------------------------------------------------


def _work_item(pair: Pair) -> dict[str, Any]:
    rows = _rows(
        "SELECT id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "tracker_display_key, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, base_branch, base_commit, cancelled_at, publication_lineage_id "
        "FROM curie.work_items WHERE tracker_kind = :kind AND tracker_issue_id = :issue",
        {"kind": pair.tracker_kind, "issue": pair.tracker_issue_id},
    )
    assert len(rows) == 1, rows
    return rows[0]


def _requests(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, sequence, status, terminal_cause, objective, dispatch_generation, "
        "reply_target_kind, reply_target_pr_number, reply_target_url "
        "FROM curie.execution_requests WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )


def _status(request_id: uuid.UUID) -> tuple[str, str | None]:
    row = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]
    return row["status"], row["terminal_cause"]


def _notice(request_id: uuid.UUID) -> dict[str, Any]:
    rows = _rows(
        "SELECT terminal_cause, posted_at, comment_id, comment_list, finalized_at, "
        "applied_label, refused_at FROM curie.factory_terminal_notices "
        "WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert len(rows) == 1, rows
    return rows[0]


def _lineage(work_item_id: uuid.UUID) -> dict[str, Any]:
    return _rows(
        "SELECT l.pr_number, l.pr_url, l.head_sha, l.branch, l.base_ref, l.code_host_pr_id, "
        "l.status FROM curie.thread_publication_lineages l "
        "JOIN curie.work_items w ON w.publication_lineage_id = l.id WHERE w.id = :id",
        {"id": work_item_id},
    )[0]


def _turns() -> list[dict[str, Any]]:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        return [
            json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
            for _id, fields in valkey.xrange(get_settings().runs_stream)
        ]
    finally:
        valkey.close()


def _drop_stream() -> None:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        valkey.delete(get_settings().runs_stream)
    finally:
        valkey.close()


def _execute_wake(request_id: uuid.UUID) -> dict[str, Any]:
    generation = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]["dispatch_generation"]
    wakes = [t for t in _turns() if t["event_id"] == execute_event_id(request_id, generation)]
    assert len(wakes) == 1, [t["event_id"] for t in _turns()]
    return wakes[0]


def _start(client: TestClient, request_id: uuid.UUID) -> None:
    """A worker acquires the published generation and starts the runtime."""

    generation = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]["dispatch_generation"]

    async def go() -> None:
        async with client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session, request_id, owner="factory-owner", generation=generation,
                wake=await factory_wake(session, request_id),
                context=None,
                client=client.app.state.http_client,
            )
            diagnostic: object = acquired
            if isinstance(acquired, DispatchConflict):
                persisted = (
                    await session.execute(
                        text(
                            "SELECT r.status, r.wait_deadline, r.base_settle_until, "
                            "r.dispatch_not_before, r.last_deferral_reason, "
                            "r.version AS request_version, r.dispatch_generation, "
                            "r.published_generation, r.acquired_generation, "
                            "r.published_generation IS DISTINCT FROM "
                            "r.dispatch_generation AS dispatch_pending, "
                            "w.version AS work_item_version, clock_timestamp() AS database_now "
                            "FROM curie.execution_requests r JOIN curie.work_items w "
                            "ON w.id = r.work_item_id WHERE r.id = :id"
                        ),
                        {"id": request_id},
                    )
                ).mappings().one_or_none()
                diagnostic = {
                    "code": acquired.code,
                    "expected_generation": generation,
                    "persisted": dict(persisted) if persisted is not None else None,
                }
            assert not isinstance(acquired, DispatchConflict), diagnostic
            started = await start(
                session,
                request_id,
                owner="factory-owner",
                generation=generation,
                claim_name="claim-factory",
                sandbox_name="sbx-factory",
                context=acquired.context,
            )
            assert not isinstance(started, DispatchConflict), started

    client.portal.call(go)


def _ci_turns(request_id: uuid.UUID) -> list[dict[str, Any]]:
    prefix = f"work-item-{request_id}-ci-"
    return [t for t in _turns() if t["event_id"].startswith(prefix)]


def _assert_one_reply(writes: list[Write], *, target: str) -> str:
    """One create, then only edits, on one target; once final, always final."""

    assert writes, "no status reply was written"
    assert [w.op for w in writes] == ["create"] + ["edit"] * (len(writes) - 1), writes
    assert {w.target for w in writes} == {target}, writes
    finals = [FINAL_MARKER in w.body for w in writes]
    assert finals[-1], writes[-1].body
    assert finals == sorted(finals), "a final reply was edited back to live"
    return writes[-1].body


def _assert_unchanged_after_final(pair: Pair, request_id: uuid.UUID, seen: list[Write]) -> None:
    """A finalized reply is never written again."""

    now = pair.writes(request_id)
    if pair.finalizes:
        assert now == seen
        return


def _agent_id() -> uuid.UUID:
    rows = _rows("SELECT id FROM curie.agents")
    assert len(rows) == 1, rows
    return rows[0]["id"]


def _create_agent(client: TestClient, *, kind: str, address: str) -> None:
    created = client.post(
        "/agents",
        headers={"X-API-Key": get_settings().api_key},
        json={
            "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
            "repo_full_name": REPO,
            "channel": {"kind": kind, "address": address},
        },
    )
    assert created.status_code == 201, created.text


# --- the shared lifecycle ----------------------------------------------------------


def _lifecycle(pair: Pair) -> None:
    # 1. The ticket is marked and admitted: one WorkItem, one waiting request.
    first = pair.mark()
    item = _work_item(pair)
    work_item_id = item["id"]
    assert (item["code_host_kind"], item["repository_project_id"], item["repository_path"]) == (
        types.GITHUB,
        str(REPO_ID),
        REPO,
    )
    assert (item["base_branch"], item["base_commit"]) == (None, None)
    assert [r["id"] for r in _requests(work_item_id)] == [first]
    assert _status(first) == ("waiting", None)
    assert _notice(first)["terminal_cause"] is None

    # 2. The reconciler publishes the execute wake and the live status reply.
    _reconcile()
    wake = _execute_wake(first)
    assert wake["text"] == _requests(work_item_id)[0]["objective"]
    assert pair.state_labels() == {"curie-factory:queued"}
    assert [w.op for w in pair.writes(first)] == ["create"]

    # 3. A worker starts the run.
    _start(pair.client, first)
    item = _work_item(pair)
    assert item["base_branch"] == "main" and item["base_commit"] is not None
    _reconcile()
    assert _status(first) == ("running", None)
    assert pair.state_labels() == {"curie-factory:running"}
    live = pair.writes(first)
    assert live[0].target == "issue"
    assert all(FINAL_MARKER not in w.body for w in live)

    # 4. The publication opens a pull request; CI on its head is red, so the
    # gate keeps the request running and enqueues fix round 2 on it.
    pr, head = pair.open_pull_request()
    _attach_publication(work_item_id, status="succeeded", pr=pr, head_sha=head)
    _set_base_ref(work_item_id, "main")
    pr_url = _lineage(work_item_id)["pr_url"]
    pair.set_ci(head, green=False)
    _reconcile()
    assert _status(first) == ("running", None)
    fix_turns = _ci_turns(first)
    assert [t["event_id"] for t in fix_turns] == [f"work-item-{first}-ci-2"]
    lines = fix_turns[0]["text"].split("\n")
    assert lines[0] == pair.issue_url
    assert lines[1] == f"Curie wait_ci round 2 of 3: the checks on {pr_url} failed at {head}."
    # The failing check is named in the fix turn (the in-memory host titles it).
    assert "unit-tests" in fix_turns[0]["text"].lower()
    assert all(FINAL_MARKER not in w.body for w in pair.writes(first))
    # Another pass while the fix turn runs enqueues nothing more.
    _reconcile()
    assert len(_ci_turns(first)) == 1
    assert _status(first) == ("running", None)

    # 5. The fix publishes a new head to the same pull request; CI is green.
    fix_head = pair.push_head("fix")
    _attach_fix(
        work_item_id,
        first,
        revision=2,
        head_sha=fix_head,
        title="Fix the widget parser off-by-one",
        paths=["src/widget.txt"],
    )
    pair.set_ci(fix_head, green=True)
    _reconcile()
    assert _status(first) == ("completed", "completed")
    assert len(_requests(work_item_id)) == 1
    assert len(_ci_turns(first)) == 1
    first_writes = pair.writes(first)
    final = _assert_one_reply(first_writes, target="issue")
    assert f"Completed: {pr_url}" in final
    assert marker_for(first) in final
    assert pair.state_labels() == {"curie-factory:pr-open"}
    assert _notice(first)["terminal_cause"] == "completed"

    # 6. Review feedback on the pull request admits a revision on the same
    # WorkItem, answered in the feedback's review thread.
    revision = pair.review(work_item_id, pr)
    rows = _requests(work_item_id)
    assert [(r["id"], r["sequence"]) for r in rows] == [(first, 1), (revision, 2)]
    assert rows[1]["status"] == "waiting"
    assert (rows[1]["reply_target_kind"], rows[1]["reply_target_pr_number"]) == (
        "review_thread",
        str(pr),
    )
    feedback_url = rows[1]["reply_target_url"]
    assert rows[1]["objective"].splitlines()[0] == feedback_url
    _reconcile()
    assert _execute_wake(revision)["text"] == rows[1]["objective"]
    assert pair.state_labels() == {"curie-factory:queued"}
    _start(pair.client, revision)
    _reconcile()
    assert _status(revision) == ("running", None)
    assert pair.state_labels() == {"curie-factory:running"}
    revision_head = pair.push_head("revision")
    _attach_fix(
        work_item_id,
        revision,
        revision=3,
        head_sha=revision_head,
        title="Rename the helper",
        paths=["src/widget.txt"],
    )
    pair.set_ci(revision_head, green=True)
    _reconcile()

    # 7. Terminal status: both requests completed, one final reply each.
    assert _status(revision) == ("completed", "completed")
    assert _status(first) == ("completed", "completed")
    reply = _assert_one_reply(pair.writes(revision), target="review_thread")
    assert marker_for(revision) in reply
    assert feedback_url in reply
    _assert_unchanged_after_final(pair, first, first_writes)
    assert pair.state_labels() == {"curie-factory:pr-open"}
    assert _notice(revision)["terminal_cause"] == "completed"
    assert _work_item(pair)["cancelled_at"] is None
    assert _lineage(work_item_id)["head_sha"] == revision_head
    # A settled pass writes nothing more.
    first_writes = pair.writes(first)
    revision_writes = pair.writes(revision)
    _reconcile()
    assert pair.writes(first) == first_writes
    assert pair.writes(revision) == revision_writes
    pair.assert_terminal(first, revision)


# --- the GitHub pair ---------------------------------------------------------------


class _ReviewGitHubAPI(GitHubAPI):
    """The intake reads plus the pull request and feedback reads the review
    truth verifier makes, on the HTML base the settings name."""

    def __init__(self) -> None:
        super().__init__()
        self.pulls: dict[int, dict[str, Any]] = {}
        self.feedback: dict[str, dict[str, Any]] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path in self.feedback:
            return httpx.Response(200, json=self.feedback[path])
        matched = re.fullmatch(rf"/repos/{REPO}/pulls/(\d+)", path)
        if matched and int(matched[1]) in self.pulls:
            return httpx.Response(200, json=self.pulls[int(matched[1])])
        return super().handle(request)


class _GitHubPair:
    tracker_kind = types.GITHUB
    finalizes = True
    tracker_issue_id = str(ISSUE_NUMBER)

    def __init__(self, client: TestClient, api: _ReviewGitHubAPI, sink: _CommentServer) -> None:
        self.client = client
        self.api = api
        self.sink = sink

    @property
    def issue_url(self) -> str:
        return f"{get_settings().github_html_base}/{REPO}/issues/{ISSUE_NUMBER}"

    def mark(self) -> uuid.UUID:
        self.api.issue_number = ISSUE_NUMBER
        self.api.advance_label_event(ISSUE_NUMBER)
        response = _post(
            self.client, "issues", _issue_event("labeled", ISSUE_NUMBER, label={"name": LABEL})
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", response.text
        rows = _requests(_work_item(self)["id"])
        assert len(rows) == 1
        return rows[0]["id"]

    def open_pull_request(self) -> tuple[int, str]:
        return PR_NUMBER, HEAD_A

    def push_head(self, stage: str) -> str:
        return {"fix": HEAD_B, "revision": HEAD_C}[stage]

    def set_ci(self, sha: str, *, green: bool) -> None:
        self.sink.ci_scripts[sha] = [ci_green() if green else ci_failing()]

    def review(self, work_item_id: uuid.UUID, pr: int) -> uuid.UUID:
        settings = get_settings()
        lineage = _lineage(work_item_id)
        html = settings.github_html_base
        api_base = settings.github_api_url.rstrip("/")
        side = {"id": REPO_ID, "full_name": REPO}
        pull = {
            "number": pr,
            "state": "open",
            "merged": False,
            "node_id": lineage["code_host_pr_id"],
            "html_url": f"{html}/{REPO}/pull/{pr}",
            "head": {"sha": lineage["head_sha"], "ref": lineage["branch"], "repo": dict(side)},
            "base": {"ref": lineage["base_ref"], "repo": dict(side)},
        }
        self.api.pulls[pr] = pull
        comment = {
            "id": FEEDBACK_ID,
            "body": REVIEW_BODY,
            "user": _sender(),
            "created_at": "2026-09-20T10:00:00Z",
            "updated_at": "2026-09-20T10:00:00Z",
            "performed_via_github_app": None,
            "author_association": "MEMBER",
            "html_url": f"{html}/{REPO}/pull/{pr}#discussion_r{FEEDBACK_ID}",
            "commit_id": lineage["head_sha"],
            "path": "src/widget.txt",
            "line": 12,
            "pull_request_review_id": FEEDBACK_ID + 500000,
        }
        current = {
            **copy.deepcopy(comment),
            "pull_request_url": f"{api_base}/repos/{REPO}/pulls/{pr}",
        }
        self.api.feedback[f"/repos/{REPO}/pulls/comments/{FEEDBACK_ID}"] = current
        payload = {
            "action": "created",
            "installation": {"id": INSTALLATION_ID},
            "repository": dict(side),
            "sender": _sender(),
            "pull_request": copy.deepcopy(pull),
            "comment": comment,
        }
        response = _post(self.client, "pull_request_review_comment", payload)
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", response.text
        rows = _requests(work_item_id)
        assert len(rows) == 2, rows
        return rows[1]["id"]

    def writes(self, request_id: uuid.UUID) -> list[Write]:
        marker = marker_for(request_id)
        found: list[Write] = []
        target = "unknown"
        for method, path, body in list(self.sink.requests):
            if method not in {"POST", "PATCH"} or not body or marker not in body:
                continue
            if method == "POST":
                if path.endswith("/replies"):
                    target = "review_thread"
                elif path == f"/repos/{REPO}/issues/{ISSUE_NUMBER}/comments":
                    target = "issue"
                else:
                    target = "pull_request"
                found.append(Write("create", target, body))
            else:
                found.append(Write("edit", target, body))
        return found

    def state_labels(self) -> set[str]:
        return set(self.sink.issue_labels.get(ISSUE_NUMBER, set())) & STATE_LABELS

    def assert_terminal(self, first: uuid.UUID, revision: uuid.UUID) -> None:
        for request_id, listed in ((first, "issue"), (revision, "review")):
            row = _notice(request_id)
            assert row["posted_at"] is not None and row["finalized_at"] is not None, row
            assert row["comment_list"] == listed
            assert row["refused_at"] is None
        assert _notice(revision)["applied_label"] == "curie-factory:pr-open"
        replies = [
            path
            for method, path, _ in self.sink.requests
            if method == "POST" and path.endswith("/replies")
        ]
        assert replies == [f"/repos/{REPO}/pulls/{PR_NUMBER}/comments/{FEEDBACK_ID}/replies"]


def _github_pair(monkeypatch: pytest.MonkeyPatch, sink: _CommentServer) -> Iterator[Pair]:
    sink.by_path = True
    # An unscripted head reads as pending, so only an explicit green completes.
    sink.ci_script = [ci_pending()]
    api = _ReviewGitHubAPI()
    with TestClient(create_app()) as client:
        external = httpx.AsyncClient(transport=httpx.MockTransport(api.handle))
        client.app.state.http_client = external
        _create_agent(client, kind="github", address=REPO)
        try:
            yield _GitHubPair(client, api, sink)
        finally:
            client.portal.call(external.aclose)
            _drop_stream()


# --- the test ----------------------------------------------------------------------


@pytest.fixture(params=[pytest.param("github", id="github")])
def pair(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[Pair]:
    yield from _github_pair(monkeypatch, request.getfixturevalue("comments"))


def test_the_factory_lifecycle_runs_offline_for_each_pairing(pair: Pair) -> None:
    _lifecycle(pair)
