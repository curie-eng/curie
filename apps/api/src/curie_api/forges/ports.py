"""The Tracker, CodeHost and MarkedComments ports (ADR 0197, "Two ports").

The factory core calls these and nothing forge-specific. Every adapter
declares each operation in ``capabilities`` (see `curie_api.forges.capabilities`),
raises only the errors in `curie_api.forges.errors`, and is run by the shared
contract suite under ``apps/api/tests/forges/contract``.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Protocol

from curie_api.forges.capabilities import Operation, Support
from curie_api.forges.types import (
    Actor,
    CiReport,
    CiRollup,
    Commit,
    Credential,
    CredentialScope,
    FeedbackPage,
    MarkedComment,
    MarkedNotice,
    NoticePage,
    PullRequest,
    PullRequestRef,
    ReplyTarget,
    RepositoryRef,
    RerunJob,
    RerunObserver,
    RerunRecord,
    ReviewFeedback,
    TrackerIssueRef,
    UpsertResult,
)


class MarkedComments(Protocol):
    """Status comments carrying a core-owned marker.

    The core owns the marker text; the adapter decides how to embed it and
    finds only comments its own identity wrote, so a human pasting the marker
    can neither be found nor overwritten.
    """

    @property
    def kind(self) -> str: ...

    @property
    def host(self) -> str: ...

    @property
    def capabilities(self) -> Mapping[Operation, Support]: ...

    async def find_marked(self, target: ReplyTarget, marker: str) -> MarkedComment | None:
        """Our own comment on ``target`` carrying ``marker``, or None.

        Raises `Unavailable` when the listing could not be read to its end:
        a missing marker is only proven by a complete listing.
        """
        ...

    async def upsert_marked(self, target: ReplyTarget, marker: str, body: str) -> UpsertResult:
        """Keep one own comment on ``target`` with ``marker`` and ``body``.

        Edits the one `find_marked` returns, or posts one. ``written`` is
        False when the stored body already matched.
        """
        ...


class Tracker(Protocol):
    """Where work is marked and described (ADR 0197, "Two ports" item 1)."""

    @property
    def kind(self) -> str: ...

    @property
    def host(self) -> str: ...

    @property
    def capabilities(self) -> Mapping[Operation, Support]: ...

    @property
    def marked_comments(self) -> MarkedComments:
        """Status comments on tracker issues (`ReplyTarget` kind ``issue``)."""
        ...

    async def poll_marked(
        self, cursor: str | None, *, running: Sequence[TrackerIssueRef]
    ) -> NoticePage:
        """Every marking since ``cursor``, plus a cancellation for any ``running``
        issue whose marker is gone, and the cursor to poll from next.

        A listing that cannot be read to its end raises `Unavailable` and
        yields no cursor, so the caller's stored cursor stays where it is.
        Events by the adapter's own identity or by bots are never notices.
        Polling the same cursor twice yields the same notices.
        """
        ...

    async def verify_current(self, notice: MarkedNotice) -> bool:
        """True when the notice still describes the issue now."""
        ...

    async def marking_actor(self, issue: TrackerIssueRef, marker: str) -> Actor | None:
        """Who applied the marker that is on the issue now, or None."""
        ...

    async def may_start(self, issue: TrackerIssueRef, actor: Actor) -> bool:
        """Whether ``actor`` may start a run on ``issue`` (ADR 0197, "Authority")."""
        ...

    async def read_ticket(self, issue: TrackerIssueRef) -> str:
        """The ticket as markdown for the sandbox. Nothing is stored."""
        ...

    async def set_state_label(
        self, issue: TrackerIssueRef, *, add: str | None, remove: Collection[str]
    ) -> None:
        """Apply the factory state label ``add`` and drop each of ``remove``."""
        ...

    async def closing_reference(self, issue: TrackerIssueRef, repository: RepositoryRef) -> str:
        """The text a pull request body carries to reference (and close) the issue."""
        ...

    async def link_pull_request(self, issue: TrackerIssueRef, pull_request: PullRequest) -> None:
        """Optional: link the pull request back to the ticket."""
        ...

    async def dependencies(self, issue: TrackerIssueRef) -> tuple[TrackerIssueRef, ...]:
        """Optional: issues this one depends on (ADR 0165)."""
        ...

    async def in_group(self, actor: Actor, group_id: str) -> bool:
        """Optional: whether ``actor`` is in the tracker group ``group_id``.

        A binding whose start authority is a group asks this (ADR 0197,
        "Authority" item 2). ``group_id`` is the tracker's immutable group id.
        """
        ...


class CodeHost(Protocol):
    """Where code, pull requests, CI and review live (ADR 0197, "Two ports" item 2)."""

    @property
    def kind(self) -> str: ...

    @property
    def host(self) -> str: ...

    @property
    def capabilities(self) -> Mapping[Operation, Support]: ...

    @property
    def marked_comments(self) -> MarkedComments:
        """Status comments on pull requests and review threads."""
        ...

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        """The repository with this immutable id and its current path, or `NotFound`."""
        ...

    async def credential(self, repository: RepositoryRef, scope: CredentialScope) -> Credential:
        """A clone- or push-scoped credential with its origin and git header."""
        ...

    async def branch_head(self, repository: RepositoryRef, branch: str) -> str | None:
        """The branch's head commit, or None when the branch does not exist."""
        ...

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit: ...

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        """The one pull request from ``head_ref`` in any state, or None.

        Raises `Ambiguous` when more than one is listed: the caller adopts one
        pull request per branch and never chooses between several.
        """
        ...

    async def open_pull_request(
        self,
        repository: RepositoryRef,
        *,
        head_ref: str,
        base_ref: str,
        title: str,
        body: str,
        draft: bool = False,
    ) -> PullRequest: ...

    async def update_pull_request(
        self, pull_request: PullRequestRef, *, title: str | None, body: str | None
    ) -> PullRequest: ...

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest: ...

    async def observe_ci(self, repository: RepositoryRef, head_sha: str) -> CiRollup:
        """Normalized checks on exactly ``head_sha``; never another commit's."""
        ...

    async def list_review_feedback(
        self, pull_request: PullRequestRef, cursor: str | None
    ) -> FeedbackPage:
        """Human feedback since ``cursor``. A partial listing raises `Unavailable`."""
        ...

    async def verify_feedback(self, feedback: ReviewFeedback) -> bool:
        """True when the feedback still exists, unchanged, by the same author, on
        an open pull request. Authority is decided separately
        (`curie_api.forges.authority.feedback_actionable`)."""
        ...

    async def ci_diagnostics(
        self, repository: RepositoryRef, head_sha: str, *, base_ref: str | None
    ) -> CiReport:
        """Optional: the checks on exactly ``head_sha``, as `observe_ci` reads
        them, with what each failing check said, from one read. When a head
        check fails and ``base_ref`` is named, ``CiReport.base`` also holds the
        checks on that branch's current head, so a failure the change did not
        cause can be told apart (#4105)."""
        ...

    async def rerun_failed(
        self,
        repository: RepositoryRef,
        jobs: Sequence[RerunJob],
        *,
        settled: Collection[str] = (),
        on_attempt: RerunObserver | None = None,
    ) -> RerunRecord:
        """Optional: rerun the failed ``jobs`` (see `types.failed_native_jobs`).

        Each rerun unit is asked once per call, skipping the units in
        ``settled`` (already accepted or refused). ``on_attempt`` sees the
        record after every answer, so a caller can store it before the next
        request is sent; returning False ends the call there.
        """
        ...

    async def user_can_write(self, repository: RepositoryRef, actor: Actor) -> bool:
        """Optional: whether ``actor`` may write to ``repository``."""
        ...


def comments_for(target: ReplyTarget, tracker: Tracker, code_host: CodeHost) -> MarkedComments:
    """The side a reply target lives on: the tracker for an issue, else the code host."""

    return tracker.marked_comments if target.kind == "issue" else code_host.marked_comments
