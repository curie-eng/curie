"""The contract harness for a Jira-shaped tracker paired with Bitbucket Data Center.

The tracker is an `InMemoryTracker` of kind ``jira_cloud`` (decision 15.1);
the code host is the real DC adapter over `BitbucketDcFake`, paging at two.
Bitbucket has no check runs and no review-with-body feedback, so every check
is a build status under ``status:<key>`` (decision 9.1) and the feedback kinds
are comments and inline comments only (decision 10).
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.memory import InMemoryTracker, PageFaults, WriteLedger, all_supported
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.types import (
    Actor,
    CheckState,
    FeedbackKind,
    PullRequestRef,
    ReplyTarget,
    TrackerIssueRef,
)

from forge_fakes.bitbucket_dc import BitbucketDcFake, dc_adapter
from forge_fakes.contract_harness import LABEL, OUTSIDER, WRITER

# Bitbucket's build states for the harness's normalized ones.
_DC_STATE = {
    CheckState.SUCCESS: "SUCCESSFUL",
    CheckState.FAILURE: "FAILED",
    CheckState.PENDING: "INPROGRESS",
    CheckState.CANCELLED: "CANCELLED",
}


def jira_like_tracker(ledger: WriteLedger, faults: PageFaults) -> InMemoryTracker:
    """The tracker side of a Bitbucket binding: Jira's declared shape."""

    caps: dict[Operation, Support] = all_supported(TRACKER_OPERATIONS)
    caps[Operation.LINK_PULL_REQUEST] = Support.NOOP
    caps[Operation.DEPENDENCIES] = Support.NOOP
    caps[Operation.GROUP_MEMBERSHIP] = Support.UNSUPPORTED
    return InMemoryTracker(
        kind=types.JIRA_CLOUD, label=LABEL, capabilities=caps, ledger=ledger, faults=faults
    )


class BitbucketDcHarness:
    """``memory-tracker+bitbucket-dc``."""

    name = "memory-tracker+bitbucket-dc"
    base_branch = "main"
    writer = WRITER
    outsider = OUTSIDER
    label = LABEL
    feedback_kinds: frozenset[FeedbackKind] = frozenset(
        {FeedbackKind.COMMENT, FeedbackKind.REVIEW_COMMENT}
    )

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Nothing to patch: the adapter's only credential source is its token.
        del monkeypatch
        self.fake = BitbucketDcFake()
        self.fake.add_user(WRITER)
        self.fake.add_user(OUTSIDER)
        self._ledger = WriteLedger()
        self._faults = PageFaults()
        self._tracker = jira_like_tracker(self._ledger, self._faults)
        self.tracker: Tracker = self._tracker
        self.code_host: CodeHost = dc_adapter(self.fake, page_size=2)
        self.repository = self.fake.repository()
        self._allowlist: set[str] = set()

    @staticmethod
    def check_key(name: str) -> str:
        """The key the adapter reports for a check reported as ``name``: every
        Bitbucket check is a build status."""

        return name if name.startswith(types.STATUS_KEY_PREFIX) else types.status_key(name)

    def seed_issue(self, title: str, body: str) -> TrackerIssueRef:
        return self._tracker.seed_issue(title, body)

    def apply_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self._tracker.apply_label(issue, LABEL, actor)

    def remove_label(self, issue: TrackerIssueRef, actor: Actor) -> None:
        self._tracker.remove_label(issue, LABEL, actor)

    def set_write_access(self, actor: Actor, allowed: bool) -> None:
        # The binding's allowlists decide; the repository permission is set too
        # so a test cannot pass by reading it (decision 11).
        self._tracker.grant(actor, allowed)
        (self._allowlist.add if allowed else self._allowlist.discard)(actor.id)
        if allowed:
            self.fake.permissions[int(actor.id)] = "REPO_WRITE"
        else:
            self.fake.permissions.pop(int(actor.id), None)

    def feedback_allowlist(self) -> frozenset[str]:
        return frozenset(self._allowlist)

    def push_head(self, branch: str) -> str:
        return self.fake.push(branch)

    def report_checks(self, sha: str, checks: Mapping[str, CheckState]) -> None:
        for name, state in checks.items():
            if state not in _DC_STATE:
                raise ValueError(f"Bitbucket has no build state for {state}")
            key = name.removeprefix(types.STATUS_KEY_PREFIX)
            self.fake.post_status(sha, key, _DC_STATE[state])

    def add_review_feedback(
        self, pull_request: PullRequestRef, author: Actor, body: str, kind: FeedbackKind
    ) -> None:
        if kind not in self.feedback_kinds:
            raise ValueError(f"Bitbucket has no {kind} feedback (#4401 decision 10)")
        pull = int(pull_request.number)
        anchor = self.fake.inline_anchor(pull) if kind is FeedbackKind.REVIEW_COMMENT else None
        self.fake.comment(pull, author, body, anchor=anchor)

    def review_thread(self, pull_request: PullRequestRef) -> str:
        pull = int(pull_request.number)
        return str(
            self.fake.comment(pull, WRITER, "Why this way?", anchor=self.fake.inline_anchor(pull))
        )

    def close_pull_request(self, pull_request: PullRequestRef, *, merged: bool) -> None:
        self.fake.set_state(int(pull_request.number), "MERGED" if merged else "DECLINED")

    def add_foreign_comment(
        self, target: ReplyTarget, author: Actor, marker: str, body: str
    ) -> None:
        if target.issue is not None:
            side = self._tracker.marked_comments
            side.seed_comment(target, author, side.embed(marker, body))
            return
        from curie_api.forges.bitbucket_dc.comments import BitbucketDcMarkedComments

        text = BitbucketDcMarkedComments.embed(marker, body)
        assert target.pull_request is not None
        parent = None if target.thread_id is None else int(target.thread_id)
        self.fake.comment(int(target.pull_request.number), author, text, parent=parent)

    def comment_bodies(self, target: ReplyTarget) -> list[tuple[Actor, str]]:
        if target.issue is not None:
            return self._tracker.marked_comments.comments(target)
        assert target.pull_request is not None
        pull = int(target.pull_request.number)
        if target.thread_id is not None:
            listed = self.fake.thread_replies(int(target.thread_id))
        else:
            listed = self.fake.top_level(pull)
        return [(self.fake.actor(c.author), c.text) for c in listed]

    def fail_next_page(self, *, after: int = 0) -> None:
        self._faults.fail_next_page(after=after)
        self.fake.fail_next_page(after=after)

    def write_count(self) -> int:
        return self._ledger.writes + self.fake.writes
