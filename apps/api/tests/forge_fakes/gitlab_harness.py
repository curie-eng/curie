"""The contract harness for GitLab as both tracker and code host (#4399 AC 3).

GitLab is a native forge: one GitLab project is the tracker scope and the
repository (ADR 0197 identity 3), and it pairs only with itself. Both adapters
are built the production way, a `ForgesConfig` through `build_registry`, then
``binding_tracker(...).for_binding`` and ``code_host(...).adapter``, and reach
`forge_fakes.gitlab.GitLabAPI` over HTTP through ``httpx.MockTransport``. The
fake pages at two, so three seeded items span two pages.

The harness acts as people on GitLab by changing the fake's state directly
(no request, no write counted), as the fixture driver does through ``Sudo``.
Each action advances the fake's clock ten minutes, so the 120 second poll
overlap (decision 8) never spans two separate human actions.

Mapping to GitLab:

1. write access is project membership at Developer (30); without it the actor
   is a Reporter (20), who can still apply labels;
2. a check is a job in the head's push pipeline; a ``status:<name>`` check is
   an external commit status ``<name>`` (decision 18);
3. a comment is a top-level merge request note (an individual-note
   discussion); a review comment is a diff thread (``individual_note: false``);
   GitLab has no review with a body, so the harness returns neither ``REVIEW``
   feedback nor a review summary (decision 20).
"""

from __future__ import annotations

from collections.abc import Mapping

import httpx
import pytest
from curie_api.forges import types
from curie_api.forges.config import ForgesConfig
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.registry import build_registry
from curie_api.forges.types import (
    Actor,
    CheckState,
    FeedbackKind,
    PullRequestRef,
    ReplyTarget,
    TrackerIssueRef,
)

from forge_fakes.contract_harness import LABEL, OUTSIDER, WRITER
from forge_fakes.gitlab import (
    BINDING_NAME,
    CODE_HOST_NAME,
    TRACKER_NAME,
    GitLabAPI,
    forges_document,
)

# GitLab's job and commit status words for the harness's normalized states.
_JOB_STATUS = {
    CheckState.SUCCESS: "success",
    CheckState.FAILURE: "failed",
    CheckState.PENDING: "running",
    CheckState.CANCELLED: "canceled",
    CheckState.SKIPPED: "skipped",
}
DEVELOPER = 30
REPORTER = 20


def embed(marker: str, body: str) -> str:
    """The marked comment body exactly as the adapter writes it (decision 15)."""

    from curie_api.forges.gitlab.comments import GitLabMarkedComments

    return GitLabMarkedComments.embed(marker, body)


class GitLabHarness:
    """``gitlab``: the GitLab tracker and code host over `GitLabAPI`."""

    name = "gitlab"
    base_branch = "main"
    writer = WRITER
    outsider = OUTSIDER
    label = LABEL
    feedback_kinds: frozenset[FeedbackKind] = frozenset(
        {FeedbackKind.COMMENT, FeedbackKind.REVIEW_COMMENT}
    )

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Nothing to patch: the adapters' only credential is the static token.
        del monkeypatch
        self.fake = GitLabAPI(page_size=2)
        for actor in (WRITER, OUTSIDER):
            self.fake.add_user(actor)
            self.fake.set_member(actor, REPORTER)
        self.registry = build_registry(ForgesConfig.model_validate(forges_document(label=LABEL)))
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(self.fake.handle))
        self.repository = self.fake.repository()
        self.tracker: Tracker = self.registry.binding_tracker(TRACKER_NAME).for_binding(
            self.client, self.registry.binding(BINDING_NAME), label=LABEL
        )
        self.code_host: CodeHost = self.registry.code_host(CODE_HOST_NAME).adapter(
            self.client, repository=self.repository
        )

    @staticmethod
    def check_key(name: str) -> str:
        return name

    def seed_issue(self, title: str, body: str) -> TrackerIssueRef:
        return self.fake.issue_ref(self.fake.add_issue(title, body, author=WRITER))

    def issue_link_suffix(self, issue: TrackerIssueRef) -> str:
        return f"/-/issues/{issue.issue_id}"

    def apply_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self.fake.label(int(issue.issue_id), LABEL, actor)

    def remove_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self.fake.label(int(issue.issue_id), LABEL, actor, add=False)

    def set_write_access(self, actor: Actor, allowed: bool) -> None:
        self.fake.set_member(actor, DEVELOPER if allowed else REPORTER)

    def feedback_allowlist(self) -> frozenset[str]:
        return frozenset()

    def push_head(self, branch: str) -> str:
        return self.fake.push(branch)

    def report_checks(self, sha: str, checks: Mapping[str, CheckState]) -> None:
        for name, state in checks.items():
            if state not in _JOB_STATUS:
                raise ValueError(f"the harness reports no GitLab status for {state}")
            if name.startswith(types.STATUS_KEY_PREFIX):
                self.fake.add_status(
                    sha, name.removeprefix(types.STATUS_KEY_PREFIX), _JOB_STATUS[state]
                )
            else:
                self.fake.report_job(sha, name, _JOB_STATUS[state])

    def add_review_feedback(
        self, pull_request: PullRequestRef, author: Actor, body: str, kind: FeedbackKind
    ) -> None:
        iid = int(pull_request.number)
        if kind is FeedbackKind.COMMENT:
            self.fake.mr_note(iid, author, body)
        elif kind is FeedbackKind.REVIEW_COMMENT:
            self.fake.mr_discussion(iid, author, body, diff=True)
        else:
            raise ValueError(f"GitLab has no {kind} feedback (#4399 decision 20)")

    def review_thread(self, pull_request: PullRequestRef) -> str:
        return self.fake.mr_discussion(int(pull_request.number), WRITER, "Why this way?")

    def close_pull_request(self, pull_request: PullRequestRef, *, merged: bool) -> None:
        self.fake.set_mr_state(int(pull_request.number), "merged" if merged else "closed")

    def add_foreign_comment(
        self, target: ReplyTarget, author: Actor, marker: str, body: str
    ) -> None:
        text = embed(marker, body)
        if target.issue is not None:
            self.fake.issue_note(int(target.issue.issue_id), author, text)
            return
        assert target.pull_request is not None
        iid = int(target.pull_request.number)
        if target.thread_id is None:
            self.fake.mr_note(iid, author, text)
        else:
            self.fake.discussion_reply(iid, target.thread_id, author, text)

    def comment_bodies(self, target: ReplyTarget) -> list[tuple[Actor, str]]:
        if target.issue is not None:
            listed = self.fake.issue_notes(int(target.issue.issue_id))
        else:
            assert target.pull_request is not None
            iid = int(target.pull_request.number)
            if target.thread_id is None:
                listed = self.fake.top_level_notes(iid)
            else:
                # The replies: every note in the thread after the one opening it.
                listed = [n for n in self.fake.thread_notes(target.thread_id)[1:] if not n.system]
        return [(self.fake.actor(note.author), note.body) for note in listed]

    def fail_next_page(self, *, after: int = 0) -> None:
        self.fake.fail_next_page(after=after)

    def write_count(self) -> int:
        return self.fake.writes
