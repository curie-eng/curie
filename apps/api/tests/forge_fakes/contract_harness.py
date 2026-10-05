"""The harness the forge contract suite drives every adapter pair through.

The suite under ``apps/api/tests/forges/contract`` calls only port methods and
this harness. Each adapter pair supplies one harness: today the in-memory pair
in two declarations; a GitHub harness backed by the fakes in this package
joins the registry in ``apps/api/tests/forges/contract/conftest.py`` without
editing any vector.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    TRACKER_OPERATIONS,
    Operation,
    Support,
)
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
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
)


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

    def add_foreign_comment(
        self, target: ReplyTarget, author: Actor, marker: str, body: str
    ) -> None:
        """``author`` posts ``body`` carrying ``marker`` embedded exactly as the
        adapter embeds it, so only identity can tell it from our own."""

    def comment_bodies(self, target: ReplyTarget) -> list[tuple[Actor, str]]: ...

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


def _conforms(harness: InMemoryHarness) -> AdapterHarness:
    """Type-checked proof that the in-memory harness satisfies the protocol."""

    return harness
