"""The whole factory lifecycle, offline, once per tracker and code host pairing (#4398).

One lifecycle runs for each adapter pair (ADR 0197, "Two ports"): a ticket is
marked, admission creates the WorkItem, the reconciler publishes the execute
wake, a worker starts the run, a publication opens a pull request, CI on its
head is red and the CI gate asks for a fix round on the same request, the fix
head is green and the request completes, review feedback admits a revision
that completes in turn, and every step's status reply and state label lands on
the side its reply target names. Persisted state is asserted at every step.

The ``github`` pair drives the GitHub fakes in ``forge_fakes``: the label and
the review feedback arrive as signed webhooks on ``/github/webhook``, and the
CI gate, status comments and labels talk HTTP to the threaded GitHub REST fake.

The ``jira_cloud+github`` pair uses the in-memory adapters in
``curie_api.forges.memory``: a ``jira_cloud`` InMemoryTracker paired with a
``github`` InMemoryCodeHost. Admission goes through the forge-neutral dispatch
functions on ``TrackerIssueRef`` and ``RepositoryRef``, the real reconciler and
CI gate read the in-memory code host, and status replies go through
``comments_for``. The adapter registry (#4397) installs no ``jira_cloud`` kind
yet, so the install boots with only a GitHub code host entry and the factory
off, and three production seams that would reach GitHub are replaced at test
level:

1. ``factory_code_host`` and ``code_host_for`` (``curie_api.forges.hosts``)
   build the GitHub adapter. Each is replaced in every module that imported it.
2. ``factory_ci._issue_url`` links the ticket through ``forges.hosts.issue_url``,
   which raises ``NotFound("tracker")`` for any tracker but GitHub, so a red CI
   round could not be continued. ``factory_ci.issue_url`` is replaced with the
   tracker port's own ``issue_url``.
3. ``factory_notices._sync_one`` returns 0 for a non-GitHub tracker. It is
   wrapped: the real function runs first and must write nothing, then the
   rendered body goes out through ``comments_for(...).upsert_marked`` and the
   real ``factory_notices._sync_labels`` sets the state label through the
   tracker port. The notice row's comment columns are GitHub-shaped, so they
   stay empty for this pair and the test asserts that.

Both pairs insert publications and fix rounds directly (the approval and
publication Job path is not offline), with the helpers the factory terminus
and CI wait suites use. Placeholder identifiers only.
"""

# ruff: noqa: F811  (the shared ``comments`` fixture is imported, then requested)

from __future__ import annotations

import asyncio
import copy
import json
import re
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx
import pytest
import redis
from aci_protocol import STREAM_PAYLOAD_FIELD
from channel_protocol.work_item_events import execute_event_id
from curie_api import factory_base, factory_notices, workitem_dispatch
from curie_api.config import get_settings
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import FINAL_MARKER
from curie_api.forges import types
from curie_api.forges.authority import feedback_actionable, may_start_with
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    Operation,
    Support,
    validate_pairing,
)
from curie_api.forges.config import WriteCheck
from curie_api.forges.hosts import github_host
from curie_api.forges.identity import issue_lock_keys, notice_request_id, revision_request_id
from curie_api.forges.memory import InMemoryCodeHost, InMemoryTracker, all_supported
from curie_api.forges.ports import comments_for
from curie_api.forges.registry import forges_registry
from curie_api.forges.types import (
    Actor,
    CheckState,
    Disposition,
    FeedbackKind,
    PullRequest,
    ReplyTarget,
)
from curie_api.github_factory_events import mentions_login
from curie_api.main import create_app
from curie_api.workitem_dispatch import Admission, DispatchConflict, acquire, start
from curie_api.workitems.lifecycle import WorkItemOutcome
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
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
    forges_document,
    forges_env_json,
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
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from test_factory_ci_wait import _attach_fix
from test_factory_terminus import (
    _attach_publication,
    _reconcile,
    _set_base_ref,
)

pytestmark = pytest.mark.usefixtures("clean_db")

STATE_LABELS = frozenset(factory_notices.STATE_LABELS)
JIRA_HOST = "example.atlassian.net"
JIRA_SCOPE = "10000"
JIRA_KEY = "PROJ-1"
MARKING_USER = Actor("6601", "acme-dev")
REVIEWER = Actor("6602", "acme-reviewer")
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


def _start(request_id: uuid.UUID) -> None:
    """A worker acquires the published generation and starts the runtime."""

    generation = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]["dispatch_generation"]

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                acquired = await acquire(
                    session, request_id, owner="factory-owner", generation=generation
                )
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="factory-owner",
                    generation=generation,
                    claim_name="claim-factory",
                    sandbox_name="sbx-factory",
                )
                assert not isinstance(started, DispatchConflict), started
        finally:
            await engine.dispose()

    asyncio.run(go())


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
    """A finalized reply is never written again.

    Without a finalized notice row (``pair.finalizes`` False) the reply is
    re-rendered on later passes; it may then only be edited, and stays final.
    """

    now = pair.writes(request_id)
    if pair.finalizes:
        assert now == seen
        return
    assert now[: len(seen)] == seen
    assert all(w.op == "edit" and FINAL_MARKER in w.body for w in now[len(seen) :]), now


def _agent_id() -> uuid.UUID:
    rows = _rows("SELECT id FROM curie.agents")
    assert len(rows) == 1, rows
    return rows[0]["id"]


def _create_agent(client: TestClient) -> None:
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
    assert item["base_branch"] == "main" and item["base_commit"] is not None
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
    _start(first)
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
    _start(revision)
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
        _create_agent(client)
        try:
            yield _GitHubPair(client, api, sink)
        finally:
            client.portal.call(external.aclose)
            _drop_stream()


# --- the jira_cloud + github pair --------------------------------------------------


@dataclass(frozen=True)
class _Logged:
    request_id: uuid.UUID
    target: str
    comment_id: str
    body: str


class _JiraPair:
    tracker_kind = types.JIRA_CLOUD
    # factory_notices._sync_one posts nothing for a non-GitHub tracker, and the
    # notice row's comment columns are GitHub-shaped, so no row is finalized.
    finalizes = False

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        settings = get_settings()
        self.tracker = InMemoryTracker(
            kind=types.JIRA_CLOUD, host=JIRA_HOST, scope_id=JIRA_SCOPE, label=LABEL, mention=MENTION
        )
        # A code host with nothing it can rerun: a red check goes straight to a fix round.
        self.code_host = InMemoryCodeHost(
            kind=types.GITHUB,
            host=github_host(settings),
            capabilities={
                **all_supported(CODE_HOST_OPERATIONS),
                Operation.RERUN_FAILED: Support.NOOP,
            },
        )
        validate_pairing(self.tracker, self.code_host)
        self.repository = self.code_host.seed_repository(REPO, project_id=str(REPO_ID))
        self.code_host.push(self.repository, "main", "initial commit")
        self.issue = self.tracker.seed_issue(
            "Widget parser drops the last field",
            "Parsing a three-field line yields two fields.",
            display_key=JIRA_KEY,
        )
        self.tracker_issue_id = self.issue.issue_id
        self.tracker.grant(MARKING_USER)
        self.code_host.grant(self.repository, REVIEWER)
        self.branch = "curie/factory-proj-1"
        self.cursor: str | None = None
        self.feedback_cursor: str | None = None
        self.pull: PullRequest | None = None
        self.conversation = f"{types.JIRA_CLOUD}-issue-{self.issue.issue_id}"
        self.log: list[_Logged] = []
        self._patch(monkeypatch)

    def _patch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The three GitHub-only seams named in the module docstring."""

        def host(*_args: Any, **_kwargs: Any) -> InMemoryCodeHost:
            return self.code_host

        for module, seams in (
            ("curie_api.forges.hosts", ("code_host_for", "factory_code_host")),
            ("curie_api.factory_ci", ("factory_code_host",)),
            ("curie_api.lineage_reconciler", ("factory_code_host",)),
            ("curie_api.repository_access", ("code_host_for",)),
            ("curie_api.github_factory", ("factory_code_host",)),
            ("curie_api.routers.publications", ("code_host_for",)),
            ("curie_api.routers.work_item_outcomes", ("factory_code_host",)),
            ("curie_api.routers.publication_code_host", ("code_host_for",)),
        ):
            for seam in seams:
                monkeypatch.setattr(f"{module}.{seam}", host)
        monkeypatch.setattr(
            "curie_api.factory_ci.issue_url",
            lambda _settings, issue, _repository: self.tracker.issue_url(issue),
        )
        monkeypatch.setattr(factory_notices, "_sync_one", self._sync_one(factory_notices._sync_one))

    def _sync_one(self, original: Callable[..., Any]) -> Callable[..., Any]:
        async def sync_one(client: httpx.AsyncClient, settings: Any, claim: Any) -> int:
            written = await original(client, settings, claim)
            if claim.tracker_issue.kind == types.GITHUB:
                return int(written)
            # The real sink is GitHub-only: it must not have written anything.
            assert written == 0
            row = claim.row
            writes = 0
            if row.finalized_at is None:
                side = comments_for(claim.target, self.tracker, self.code_host)
                result = await side.upsert_marked(
                    claim.target, f"curie-factory-status:{row.execution_request_id}", claim.body
                )
                if result.written:
                    self.log.append(
                        _Logged(
                            row.execution_request_id,
                            claim.target.kind,
                            result.comment.id,
                            claim.body,
                        )
                    )
                    writes += 1
            if claim.latest and row.refused_at is None:
                writes += await factory_notices._sync_labels(self.tracker, claim)
            return writes

        return sync_one

    @property
    def issue_url(self) -> str:
        return self.tracker.issue_url(self.issue)

    async def _dispatch(self, facts: Admission, *, revision: bool) -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                classid, objid = issue_lock_keys(facts.issue)
                await session.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "CAST(:classid AS integer), CAST(:objid AS integer))"
                    ),
                    {"classid": classid, "objid": objid},
                )
                admit = workitem_dispatch.admit_revision if revision else workitem_dispatch.readmit
                result = await admit(session, facts)
                await session.commit()
        finally:
            await engine.dispose()
        assert isinstance(result, WorkItemOutcome), result
        assert not result.replayed
        assert result.request is not None and result.request.id == facts.request_id
        return facts.request_id

    def mark(self) -> uuid.UUID:
        self.tracker.apply_label(self.issue, LABEL, MARKING_USER)
        agent_id = _agent_id()

        async def go() -> uuid.UUID:
            page = await self.tracker.poll_marked(self.cursor, running=())
            notices = [n for n in page.notices if n.disposition is Disposition.ADMIT]
            assert len(notices) == 1, page
            notice = notices[0]
            assert notice.issue == self.issue
            assert await self.tracker.verify_current(notice)
            actor = await self.tracker.marking_actor(self.issue, LABEL)
            assert actor == notice.actor == MARKING_USER
            assert await may_start_with(WriteCheck(), self.issue, actor, tracker=self.tracker)
            self.cursor = page.cursor
            base = await factory_base.resolve_base(
                registry=forges_registry(get_settings()),
                repo_full_name=REPO,
                labels=set(),
                default_branch="main",
                code_host=self.code_host,
                repository=self.repository,
            )
            assert isinstance(base, factory_base.ResolvedBase), base
            return await self._dispatch(
                Admission(
                    agent_id=agent_id,
                    kind="github",
                    address=REPO,
                    reply_conversation_id=self.conversation,
                    issue=self.issue,
                    repository=self.repository,
                    code_host_installation_id=INSTALLATION_ID,
                    objective=self.tracker.issue_url(self.issue),
                    requester=f"{types.JIRA_CLOUD}:{actor.id}:{actor.login}",
                    request_id=notice_request_id(notice),
                    base=base,
                ),
                revision=False,
            )

        request_id = asyncio.run(go())
        item = _work_item(self)
        assert (item["tracker_host"], item["tracker_scope_id"], item["tracker_display_key"]) == (
            JIRA_HOST,
            JIRA_SCOPE,
            JIRA_KEY,
        )
        return request_id

    def open_pull_request(self) -> tuple[int, str]:
        async def go() -> PullRequest:
            self.code_host.push(self.repository, self.branch, "Fix the widget parser")
            assert (
                await self.code_host.find_pull_request(self.repository, head_ref=self.branch)
                is None
            )
            closing = await self.tracker.closing_reference(self.issue, self.repository)
            pull = await self.code_host.open_pull_request(
                self.repository,
                head_ref=self.branch,
                base_ref="main",
                title="Fix the widget parser",
                body=f"{closing}\n\nKeeps the last field of a line.",
            )
            await self.tracker.link_pull_request(self.issue, pull)
            return pull

        self.pull = asyncio.run(go())
        return int(self.pull.ref.number), self.pull.head_sha

    def push_head(self, stage: str) -> str:
        return self.code_host.push(self.repository, self.branch, stage)

    def set_ci(self, sha: str, *, green: bool) -> None:
        failing = CheckState.SUCCESS if green else CheckState.FAILURE
        self.code_host.report_checks(
            sha,
            {"unit-tests": failing, "lint": CheckState.SUCCESS},
            excerpt=None if green else "test_widget_parses_input failed: expected 3, got 2",
        )

    def review(self, work_item_id: uuid.UUID, pr: int) -> uuid.UUID:
        pull = self.pull
        assert pull is not None and pull.ref.number == str(pr)
        agent_id = _agent_id()

        async def go() -> uuid.UUID:
            seeded = self.code_host.seed_feedback(
                pull.ref, REVIEWER, REVIEW_BODY, FeedbackKind.REVIEW_COMMENT, thread_id="7701"
            )
            page = await self.code_host.list_review_feedback(pull.ref, self.feedback_cursor)
            self.feedback_cursor = page.cursor
            asked = [item for item in page.items if mentions_login(item.body, MENTION)]
            assert asked == [seeded]
            feedback = asked[0]
            assert await feedback_actionable(self.code_host, feedback, allowlist=())
            assert feedback.thread_id is not None
            url = f"{pull.url}#discussion_r{feedback.id}"
            provenance = {
                "event": FeedbackKind.REVIEW_COMMENT.value,
                "url": url,
                "sender": feedback.author.login,
                "body": feedback.body,
            }
            return await self._dispatch(
                Admission(
                    agent_id=agent_id,
                    kind="github",
                    address=REPO,
                    reply_conversation_id=self.conversation,
                    issue=self.issue,
                    repository=self.repository,
                    code_host_installation_id=INSTALLATION_ID,
                    objective=(
                        f"{url}\n\nReview feedback asked for another revision.\n"
                        + json.dumps(provenance)
                    ),
                    requester=f"{types.GITHUB}:{feedback.author.id}:{feedback.author.login}",
                    request_id=revision_request_id(self.repository, feedback.kind, feedback.id),
                    reply_target=ReplyTarget.on_thread(pull.ref, feedback.thread_id),
                    reply_url=url,
                ),
                revision=True,
            )

        return asyncio.run(go())

    def writes(self, request_id: uuid.UUID) -> list[Write]:
        seen: set[str] = set()
        found: list[Write] = []
        for entry in self.log:
            if entry.request_id != request_id:
                continue
            found.append(
                Write("edit" if entry.comment_id in seen else "create", entry.target, entry.body)
            )
            seen.add(entry.comment_id)
        return found

    def state_labels(self) -> set[str]:
        return set(self.tracker.labels(self.issue)) & STATE_LABELS

    def assert_terminal(self, first: uuid.UUID, revision: uuid.UUID) -> None:
        assert self.pull is not None
        # The issue reply lives on the tracker, the thread reply on the code host.
        issue_target = ReplyTarget.on_issue(self.issue)
        on_issue = self.tracker.marked_comments.comments(issue_target)
        assert len(on_issue) == 1 and on_issue[0][0] == self.tracker.identity
        assert marker_for(first) in on_issue[0][1] and FINAL_MARKER in on_issue[0][1]
        thread = ReplyTarget.on_thread(self.pull.ref, "7701")
        on_thread = self.code_host.marked_comments.comments(thread)
        assert len(on_thread) == 1 and marker_for(revision) in on_thread[0][1]
        # The pull request closes and links the ticket by its display key.
        current = asyncio.run(self.code_host.read_pull_request(self.pull.ref))
        assert current.body.startswith(f"Closes {JIRA_KEY}")
        assert self.tracker.linked(self.issue) == (self.pull.ref,)
        assert _lineage(_work_item(self)["id"])["pr_url"] == self.pull.url
        # The state labels the sink wrote are the bot's own: no new notice, and
        # the factory label still on the ticket cancels nothing.
        page = asyncio.run(self.tracker.poll_marked(self.cursor, running=(self.issue,)))
        assert page.notices == ()
        assert LABEL in self.tracker.labels(self.issue)
        # The notice row's comment columns are GitHub-shaped (an integer id and
        # an 'issue' or 'review' list), so the port reply leaves them empty;
        # the label the sink applied is persisted.
        for request_id in (first, revision):
            row = _notice(request_id)
            assert row["posted_at"] is None and row["comment_id"] is None, row
            assert row["terminal_cause"] == "completed"
        assert _notice(revision)["applied_label"] == "curie-factory:pr-open"


def _jira_pair(monkeypatch: pytest.MonkeyPatch, key_dir: Path) -> Iterator[Pair]:
    for key, value in {
        "CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS": "30",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        # No GitHub intake: the reconciler must not poll a GitHub tracker.
        "FACTORY_ENABLED": "false",
        "CURIE_FORGES": forges_env_json(forges_document(), key_dir),
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "GITHUB_TOKEN": "",
        "INTERNAL_WORKER_TOKEN": "factory-terminus-worker",
        "RUNS_STREAM": f"test:curie:offline-lifecycle:{uuid.uuid4().hex}",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("GITHUB_API_URL", raising=False)
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            _create_agent(client)
            try:
                yield _JiraPair(monkeypatch)
            finally:
                _clear_ci_keys()
                _drop_stream()
    finally:
        get_settings.cache_clear()


# --- the test ----------------------------------------------------------------------


@pytest.fixture(
    params=[
        pytest.param("github", id="github"),
        pytest.param("jira_cloud+github", id="jira_cloud+github"),
    ]
)
def pair(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[Pair]:
    if request.param == "github":
        yield from _github_pair(monkeypatch, request.getfixturevalue("comments"))
    else:
        yield from _jira_pair(monkeypatch, request.getfixturevalue("tmp_path"))


def test_the_factory_lifecycle_runs_offline_for_each_pairing(pair: Pair) -> None:
    _lifecycle(pair)
