"""The harness the forge contract suite drives every adapter pair through.

The suite under ``apps/api/tests/forges/contract`` calls only port methods and
this harness. Each adapter pair supplies one harness: today the in-memory pair
in two declarations, and the GitHub tracker and code host over the fake in
``forge_fakes/github.py``. Each joins the registry in
``apps/api/tests/forges/contract/conftest.py`` without editing any vector.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from typing import Any, Protocol

import httpx
import pytest
from curie_api.config import Settings
from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    TRACKER_OPERATIONS,
    Operation,
    Support,
)
from curie_api.forges.github import ci as github_ci
from curie_api.forges.github import code_host as github_code_host
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.github.comments import GitHubMarkedComments, static_token
from curie_api.forges.github.tracker import GitHubTracker
from curie_api.forges.memory import (
    InMemoryCodeHost,
    InMemoryMarkedComments,
    InMemoryTracker,
    PageFaults,
    WriteLedger,
    all_supported,
)
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.types import (
    Actor,
    CheckState,
    FeedbackKind,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
)

from forge_fakes.github import CONTRACT_APP_ID, REPO, REPO_ID, GitHubRepositoryFake


class AdapterHarness(Protocol):
    """Seeds and observes one tracker and code host pair from outside the ports."""

    name: str
    tracker: Tracker
    code_host: CodeHost
    label: str
    repository: RepositoryRef
    base_branch: str
    writer: Actor
    outsider: Actor

    def seed_issue(self, title: str, body: str) -> TrackerIssueRef: ...

    def apply_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        """``actor`` applies the factory label."""

    def remove_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        """``actor`` removes the factory label."""

    def set_write_access(self, actor: Actor, allowed: bool) -> None:
        """Grant or revoke the authority the pairing checks: repository write
        access, and on a tracker-only pairing the binding's allowlists too."""

    def feedback_allowlist(self) -> frozenset[str]:
        """The binding's code-host account allowlist for review feedback."""

    def push_head(self, branch: str) -> str:
        """Push a new commit to ``branch``; returns its sha."""

    def report_checks(self, sha: str, checks: Mapping[str, CheckState]) -> None: ...

    def add_review_feedback(
        self, pull_request: PullRequestRef, author: Actor, body: str, kind: FeedbackKind
    ) -> None: ...

    def review_thread(self, pull_request: PullRequestRef) -> str:
        """The id of a review thread a person opened on ``pull_request``."""

    def close_pull_request(self, pull_request: PullRequestRef, *, merged: bool) -> None:
        """Someone closes ``pull_request``, merging it when ``merged``."""

    def add_foreign_comment(
        self, target: ReplyTarget, author: Actor, marker: str, body: str
    ) -> None:
        """``author`` posts ``body`` carrying ``marker`` embedded exactly as the
        adapter embeds it, so only identity can tell it from our own."""

    def comment_bodies(self, target: ReplyTarget) -> list[tuple[Actor, str]]:
        """Comments posted on ``target``; on a thread, the replies in it."""

    def fail_next_page(self, *, after: int = 0) -> None:
        """The listing page read after ``after`` more successful reads fails once.

        Listings page at two items, so three seeded items span two pages."""

    def write_count(self) -> int:
        """Forge writes made through port calls so far."""


async def open_pull(harness: AdapterHarness, branch: str) -> PullRequest:
    """Push ``branch`` and open a pull request from it through the port."""

    harness.push_head(branch)
    return await harness.code_host.open_pull_request(
        harness.repository,
        head_ref=branch,
        base_ref=harness.base_branch,
        title="Factory change",
        body="Change body",
    )


WRITER = Actor(id="6601", login="octo-writer")
OUTSIDER = Actor(id="6602", login="drive-by")
LABEL = "factory"


class InMemoryHarness:
    """The in-memory pair. ``minimal`` declares the optional operations no-op or
    unsupported and pairs a tracker-only kind, the shape a Jira and Bitbucket
    binding has, so the suite proves the no-op and fallback paths too."""

    base_branch = "main"
    writer = WRITER
    outsider = OUTSIDER
    label = LABEL

    def __init__(self, *, minimal: bool) -> None:
        self.name = "memory-minimal" if minimal else "memory"
        self._ledger = WriteLedger()
        self._faults = PageFaults()
        tracker_caps: dict[Operation, Support] = all_supported(TRACKER_OPERATIONS)
        code_caps: dict[Operation, Support] = all_supported(CODE_HOST_OPERATIONS)
        if minimal:
            tracker_caps[Operation.LINK_PULL_REQUEST] = Support.NOOP
            tracker_caps[Operation.DEPENDENCIES] = Support.NOOP
            tracker_caps[Operation.GROUP_MEMBERSHIP] = Support.UNSUPPORTED
            code_caps[Operation.RERUN_FAILED] = Support.NOOP
            code_caps[Operation.CI_DIAGNOSTICS] = Support.UNSUPPORTED
            code_caps[Operation.USER_CAN_WRITE] = Support.UNSUPPORTED
        self._tracker = InMemoryTracker(
            kind=types.MEMORY_TRACKER_ONLY if minimal else types.MEMORY,
            label=LABEL,
            capabilities=tracker_caps,
            ledger=self._ledger,
            faults=self._faults,
        )
        self._code_host = InMemoryCodeHost(
            capabilities=code_caps, ledger=self._ledger, faults=self._faults
        )
        self.tracker: Tracker = self._tracker
        self.code_host: CodeHost = self._code_host
        self.repository = self._code_host.seed_repository("acme-corp/acme-bot")
        self._code_host.push(self.repository, self.base_branch, "initial")
        self._allowlist: set[str] = set()
        self._minimal = minimal

    def seed_issue(self, title: str, body: str) -> TrackerIssueRef:
        return self._tracker.seed_issue(title, body)

    def apply_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self._tracker.apply_label(issue, LABEL, actor)

    def remove_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self._tracker.remove_label(issue, LABEL, actor)

    def set_write_access(self, actor: Actor, allowed: bool) -> None:
        self._tracker.grant(actor, allowed)
        self._code_host.grant(self.repository, actor, allowed)
        if self._minimal:
            (self._allowlist.add if allowed else self._allowlist.discard)(actor.id)

    def feedback_allowlist(self) -> frozenset[str]:
        return frozenset(self._allowlist)

    def push_head(self, branch: str) -> str:
        return self._code_host.push(self.repository, branch)

    def report_checks(self, sha: str, checks: Mapping[str, CheckState]) -> None:
        self._code_host.report_checks(sha, checks, excerpt="assertion failed")

    def add_review_feedback(
        self, pull_request: PullRequestRef, author: Actor, body: str, kind: FeedbackKind
    ) -> None:
        thread = "t1" if kind is FeedbackKind.REVIEW_COMMENT else None
        self._code_host.seed_feedback(pull_request, author, body, kind, thread_id=thread)

    def review_thread(self, pull_request: PullRequestRef) -> str:
        return "t1"

    def close_pull_request(self, pull_request: PullRequestRef, *, merged: bool) -> None:
        state = PullRequestState.MERGED if merged else PullRequestState.CLOSED
        self._code_host.set_state(pull_request, state)

    def _comments_side(self, target: ReplyTarget) -> InMemoryMarkedComments:
        if target.kind == "issue":
            return self._tracker.marked_comments
        return self._code_host.marked_comments

    def add_foreign_comment(
        self, target: ReplyTarget, author: Actor, marker: str, body: str
    ) -> None:
        side = self._comments_side(target)
        side.seed_comment(target, author, side.embed(marker, body))

    def comment_bodies(self, target: ReplyTarget) -> list[tuple[Actor, str]]:
        return self._comments_side(target).comments(target)

    def fail_next_page(self, *, after: int = 0) -> None:
        self._faults.fail_next_page(after=after)

    def write_count(self) -> int:
        return self._ledger.writes


def _github_user(actor: Actor) -> dict[str, Any]:
    return {"id": int(actor.id), "login": actor.login, "type": "User"}


_GIT_HEADER = "Basic " + base64.b64encode(b"x-access-token:fixture-installation-token").decode()


class _AppCredentials:
    """The App installation token mint, without the App (`curie_api.github_app`)."""

    app_configured = True

    def fresh_installation_token(
        self, repo_full_name: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        assert repo_full_name == REPO
        return 5501, "fixture-installation-token"


class GitHubHarness:
    """The GitHub tracker and code host over `GitHubRepositoryFake`, paging at two.

    The code host's two credential sources, the repository git credential and
    the App installation token mint, are replaced with fixtures; every other
    call goes over HTTP to the fake.
    """

    name = "github"
    base_branch = "main"
    writer = WRITER
    outsider = OUTSIDER
    label = LABEL

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            github_code_host,
            "resolve_repository_credential",
            lambda path, settings: (f"{settings.github_html_base}/{path}.git", _GIT_HEADER),
        )
        monkeypatch.setattr(github_ci, "credentials_for", lambda _settings: _AppCredentials())
        self.fake = GitHubRepositoryFake()
        client = httpx.AsyncClient(transport=httpx.MockTransport(self.fake.handle))
        token = static_token("fixture-installation-token")
        self._tracker = GitHubTracker(
            client,
            api="https://api.github.com",
            html_base="https://github.com",
            repo_full_name=REPO,
            repository_id=REPO_ID,
            token=token,
            label=LABEL,
            mention="curie",
            app_id=str(CONTRACT_APP_ID),
            page_size=2,
        )
        settings = Settings(github_api_url="https://api.github.com")
        self._code_host = GitHubCodeHost(
            settings,
            client,
            marked_comments=GitHubMarkedComments(
                client,
                api="https://api.github.com",
                host="github.com",
                repo_full_name=REPO,
                repository_id=REPO_ID,
                token=token,
                app_id=str(CONTRACT_APP_ID),
            ),
            repository_paths={str(REPO_ID): REPO},
            page_size=2,
        )
        self.tracker: Tracker = self._tracker
        self.code_host: CodeHost = self._code_host
        self.repository = RepositoryRef(
            types.GITHUB, self._code_host.host, str(REPO_ID), REPO, default_branch="main"
        )

    def seed_issue(self, title: str, body: str) -> TrackerIssueRef:
        return self._tracker.issue(self.fake.seed_issue(title, body))

    def apply_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self.fake.label_event(int(issue.issue_id), "labeled", LABEL, _github_user(actor))

    def remove_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self.fake.label_event(int(issue.issue_id), "unlabeled", LABEL, _github_user(actor))

    def set_write_access(self, actor: Actor, allowed: bool) -> None:
        self.fake.permissions[actor.login] = (int(actor.id), "write" if allowed else "read")

    def feedback_allowlist(self) -> frozenset[str]:
        return frozenset()

    def push_head(self, branch: str) -> str:
        return self.fake.push_head(branch)

    def report_checks(self, sha: str, checks: Mapping[str, CheckState]) -> None:
        for key, state in checks.items():
            self.fake.report_check(sha, key, state.value)

    def add_review_feedback(
        self, pull_request: PullRequestRef, author: Actor, body: str, kind: FeedbackKind
    ) -> None:
        number, user = int(pull_request.number), _github_user(author)
        if kind is FeedbackKind.COMMENT:
            self.fake.add_comment(number, user, body, app=False)
        elif kind is FeedbackKind.REVIEW_COMMENT:
            self.fake.add_review_comment(number, user, body, app=False)
        else:
            self.fake.add_review(number, user, body)

    def review_thread(self, pull_request: PullRequestRef) -> str:
        opened = self.fake.add_review_comment(
            int(pull_request.number), _github_user(WRITER), "Why this way?", app=False
        )
        return str(opened["id"])

    def close_pull_request(self, pull_request: PullRequestRef, *, merged: bool) -> None:
        self.fake.set_pull_state(int(pull_request.number), merged=merged)

    def add_foreign_comment(
        self, target: ReplyTarget, author: Actor, marker: str, body: str
    ) -> None:
        text = GitHubMarkedComments.embed(marker, body)
        user = _github_user(author)
        if target.issue is not None:
            self.fake.add_comment(int(target.issue.issue_id), user, text, app=False)
            return
        assert target.pull_request is not None
        number = int(target.pull_request.number)
        if target.thread_id is None:
            self.fake.add_comment(number, user, text, app=False)
        else:
            self.fake.add_review_comment(
                number, user, text, app=False, in_reply_to=int(target.thread_id)
            )

    def comment_bodies(self, target: ReplyTarget) -> list[tuple[Actor, str]]:
        if target.thread_id is not None:
            thread = int(target.thread_id)
            listed = [c for c in self.fake.review_comments if c.get("in_reply_to_id") == thread]
        else:
            ref = target.issue.issue_id if target.issue is not None else None
            if ref is None:
                assert target.pull_request is not None
                ref = target.pull_request.number
            listed = [c for c in self.fake.comments if c["_number"] == int(ref)]
        return [(Actor(str(c["user"]["id"]), c["user"]["login"]), c["body"]) for c in listed]

    def fail_next_page(self, *, after: int = 0) -> None:
        self.fake.fail_next_page(after=after)

    def write_count(self) -> int:
        return self.fake.writes


def _conforms(harness: InMemoryHarness | GitHubHarness) -> AdapterHarness:
    """Type-checked proof that each harness satisfies the protocol."""

    return harness
