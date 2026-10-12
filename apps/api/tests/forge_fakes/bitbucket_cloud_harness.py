"""The ``memory-tracker+bitbucket-cloud`` pair of the forge contract suite (#4401, AC 1).

A ``jira_cloud`` InMemoryTracker (Bitbucket pairs only with Jira, decision 2)
and the real Bitbucket Cloud adapter over `BitbucketCloudFake`, paging at two.
Bitbucket has no check runs and no review-with-body feedback, so a check
reported as ``unit`` comes back as the status key ``status:unit`` and review
feedback is comments only (decisions 9.1 and 10). Its code host cannot report
write access, so authority is the binding's allowlist this harness keeps.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest
from curie_api.forges import types
from curie_api.forges.memory import PageFaults, WriteLedger
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.types import (
    Actor,
    CheckState,
    FeedbackKind,
    PullRequestRef,
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
    status_key,
)

from forge_fakes.bitbucket_cloud import (
    FACTORY_TOKEN,
    INLINE,
    BitbucketCloudFake,
)
from forge_fakes.bitbucket_cloud_record import adapter_for
from forge_fakes.bitbucket_lifecycle import memory_jira_tracker
from forge_fakes.contract_harness import LABEL, OUTSIDER, WRITER

# Bitbucket Cloud's build states for the contract's check states. Neutral and
# skipped have no Bitbucket word, so a vector reporting them fails loudly here.
NATIVE_STATES: Mapping[CheckState, str] = {
    CheckState.SUCCESS: "SUCCESSFUL",
    CheckState.FAILURE: "FAILED",
    CheckState.PENDING: "INPROGRESS",
    CheckState.CANCELLED: "STOPPED",
}


def embed(marker: str, body: str) -> str:
    """A marked body exactly as the adapter writes one."""

    from curie_api.forges.bitbucket_cloud.comments import BitbucketCloudMarkedComments

    text: str = BitbucketCloudMarkedComments.embed(marker, body)
    return text


class BitbucketCloudHarness:
    """A Jira-kind memory tracker with the Cloud adapter over the fake."""

    name = "memory-tracker+bitbucket-cloud"
    base_branch = "main"
    writer = WRITER
    outsider = OUTSIDER
    label = LABEL
    feedback_kinds = frozenset({FeedbackKind.COMMENT, FeedbackKind.REVIEW_COMMENT})

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._ledger = WriteLedger()
        self._faults = PageFaults()
        self._tracker = memory_jira_tracker(ledger=self._ledger, faults=self._faults)
        self.fake = BitbucketCloudFake()
        for actor in (WRITER, OUTSIDER):
            self.fake.add_user(actor)
        self.tracker: Tracker = self._tracker
        self.code_host: CodeHost = adapter_for(self.fake.client(), FACTORY_TOKEN)
        self.repository: RepositoryRef = self.fake.repository()
        self._allowlist: set[str] = set()

    def check_key(self, name: str) -> str:
        return status_key(name.removeprefix(types.STATUS_KEY_PREFIX))

    def seed_issue(self, title: str, body: str) -> TrackerIssueRef:
        return self._tracker.seed_issue(title, body)

    def issue_link_suffix(self, issue: TrackerIssueRef) -> str:
        return f"/issues/{issue.issue_id}"

    def apply_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self._tracker.apply_label(issue, LABEL, actor)

    def remove_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self._tracker.remove_label(issue, LABEL, actor)

    def set_write_access(self, actor: Actor, allowed: bool) -> None:
        self._tracker.grant(actor, allowed)
        (self._allowlist.add if allowed else self._allowlist.discard)(actor.id)
        (self.fake.writers.add if allowed else self.fake.writers.discard)(actor.id)

    def feedback_allowlist(self) -> frozenset[str]:
        return frozenset(self._allowlist)

    def push_head(self, branch: str) -> str:
        return self.fake.push(branch, f"Change on {branch}")

    def report_checks(self, sha: str, checks: Mapping[str, CheckState]) -> None:
        for name, state in checks.items():
            key = name.removeprefix(types.STATUS_KEY_PREFIX)
            self.fake.post_status(sha, key, NATIVE_STATES[state])

    def add_review_feedback(
        self, pull_request: PullRequestRef, author: Actor, body: str, kind: FeedbackKind
    ) -> None:
        if kind not in self.feedback_kinds:
            raise AssertionError(f"Bitbucket has no {kind} feedback (#4401 decision 10)")
        inline = dict(INLINE) if kind is FeedbackKind.REVIEW_COMMENT else None
        self.fake.comment(int(pull_request.number), author, body, inline=inline)

    def review_thread(self, pull_request: PullRequestRef) -> str:
        opened = self.fake.comment(
            int(pull_request.number), WRITER, "Why this way?", inline=dict(INLINE)
        )
        return str(opened)

    def close_pull_request(self, pull_request: PullRequestRef, *, merged: bool) -> None:
        self.fake.set_state(int(pull_request.number), "MERGED" if merged else "DECLINED")

    def add_foreign_comment(
        self, target: ReplyTarget, author: Actor, marker: str, body: str
    ) -> None:
        if target.issue is not None:
            side = self._tracker.marked_comments
            side.seed_comment(target, author, side.embed(marker, body))
            return
        assert target.pull_request is not None
        parent = None if target.thread_id is None else int(target.thread_id)
        self.fake.comment(
            int(target.pull_request.number), author, embed(marker, body), parent=parent
        )

    def comment_bodies(self, target: ReplyTarget) -> list[tuple[Actor, str]]:
        if target.issue is not None:
            return self._tracker.marked_comments.comments(target)
        assert target.pull_request is not None
        if target.thread_id is not None:
            listed = self.fake.thread_replies(int(target.thread_id))
        else:
            listed = self.fake.conversation(int(target.pull_request.number))
        return [(c.author, c.raw) for c in listed]

    def fail_next_page(self, *, after: int = 0) -> None:
        self._faults.fail_next_page(after=after)
        self.fake.fail_next_page(after=after)

    def write_count(self) -> int:
        return self._ledger.writes + self.fake.writes
