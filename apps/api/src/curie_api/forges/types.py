"""Forge-neutral values that cross the Tracker and CodeHost ports (ADR 0197).

Nothing here names a GitHub field. Every identifier is text, because GitHub
numbers, GitLab iids, Jira numeric ids and Bitbucket ids do not share a type.
Equality follows the ADR's identity rules: a display key or a path is carried
for display and never takes part in comparison.
"""

from __future__ import annotations

import base64
import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

# Forge kinds. Only GitHub has an adapter today; the other kinds are named so
# the pairing rules in `capabilities` can be stated before their adapters land.
GITHUB = "github"
GITLAB = "gitlab"
JIRA_CLOUD = "jira_cloud"
BITBUCKET_CLOUD = "bitbucket_cloud"
BITBUCKET_DC = "bitbucket_dc"
# The in-memory adapters in `curie_api.forges.memory`. `MEMORY` is a native
# forge (both ports); `MEMORY_TRACKER_ONLY` is a tracker-only kind that pairs
# with any code host, the shape Jira has.
MEMORY = "memory"
MEMORY_TRACKER_ONLY = "memory-tracker-only"


def _require_text(owner: str, **values: str) -> None:
    for name, value in values.items():
        if not isinstance(value, str) or not value:
            raise ValueError(f"{owner}.{name} must be non-empty text")


@dataclass(frozen=True)
class TrackerIssueRef:
    """One tracker issue, keyed by (kind, host, scope id, issue id).

    The scope is the immutable boundary the issue id is unique within and
    survives a move: the repository id on GitHub, the project id on GitLab,
    the site (cloudId) on Jira. The issue id is the tracker's immutable id as
    text. ``display_key`` (a Jira key such as ``PROJ-123``) changes when the
    issue moves, so it is never part of equality or hashing.
    """

    kind: str
    host: str
    scope_id: str
    issue_id: str
    display_key: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        _require_text(
            "TrackerIssueRef",
            kind=self.kind,
            host=self.host,
            scope_id=self.scope_id,
            issue_id=self.issue_id,
        )


@dataclass(frozen=True)
class RepositoryRef:
    """One repository, bound by (code host kind, host, immutable project id).

    ``path`` is for display and cloning. Before cloning, the adapter resolves
    the path from the id and refuses a mismatch; the path never decides identity.
    """

    kind: str
    host: str
    project_id: str
    path: str = field(compare=False)

    def __post_init__(self) -> None:
        _require_text(
            "RepositoryRef",
            kind=self.kind,
            host=self.host,
            project_id=self.project_id,
            path=self.path,
        )


@dataclass(frozen=True)
class Actor:
    """A forge account. The immutable id decides identity; the login is display."""

    id: str
    login: str = field(compare=False)

    def __post_init__(self) -> None:
        _require_text("Actor", id=self.id, login=self.login)


class PullRequestState(enum.StrEnum):
    OPEN = "open"
    MERGED = "merged"
    CLOSED = "closed"


@dataclass(frozen=True)
class PullRequestRef:
    """A pull or merge request: its repository and its number or iid as text."""

    repository: RepositoryRef
    number: str

    def __post_init__(self) -> None:
        _require_text("PullRequestRef", number=self.number)


@dataclass(frozen=True)
class PullRequest:
    ref: PullRequestRef
    head_sha: str
    head_ref: str
    base_ref: str
    state: PullRequestState
    url: str
    title: str = ""
    body: str = ""


@dataclass(frozen=True)
class Commit:
    sha: str
    parents: tuple[str, ...]
    message: str


ReplyKind = Literal["issue", "pull_request", "review_thread"]


@dataclass(frozen=True)
class ReplyTarget:
    """Where a factory status reply lands: a tracker issue, a pull request
    conversation, or one review thread on a pull request.

    With Jira and Bitbucket the first lives on a different system from the
    other two, which is why the target is typed rather than a URL.
    """

    kind: ReplyKind
    issue: TrackerIssueRef | None = None
    pull_request: PullRequestRef | None = None
    thread_id: str | None = None

    def __post_init__(self) -> None:
        if self.kind == "issue":
            valid = self.issue is not None and self.pull_request is None and self.thread_id is None
        elif self.kind == "pull_request":
            valid = self.issue is None and self.pull_request is not None and self.thread_id is None
        else:
            valid = self.issue is None and self.pull_request is not None and bool(self.thread_id)
        if not valid:
            raise ValueError(f"ReplyTarget of kind {self.kind!r} has the wrong references")

    @classmethod
    def on_issue(cls, issue: TrackerIssueRef) -> ReplyTarget:
        return cls("issue", issue=issue)

    @classmethod
    def on_pull_request(cls, pull_request: PullRequestRef) -> ReplyTarget:
        return cls("pull_request", pull_request=pull_request)

    @classmethod
    def on_thread(cls, pull_request: PullRequestRef, thread_id: str) -> ReplyTarget:
        return cls("review_thread", pull_request=pull_request, thread_id=thread_id)


class CredentialHeader(enum.StrEnum):
    """The header form git accepts on a forge. GitLab refuses a Bearer header."""

    AUTHORIZATION_BASIC = "authorization_basic"
    AUTHORIZATION_BEARER = "authorization_bearer"
    PRIVATE_TOKEN = "private_token"


class CredentialScope(enum.StrEnum):
    CLONE = "clone"
    PUSH = "push"


class CredentialExpiry(enum.StrEnum):
    """Whether a credential's expiry is known, unknown (a static token whose
    lifetime the forge does not report), or absent (it never expires)."""

    KNOWN = "known"
    UNKNOWN = "unknown"
    NONE = "none"


@dataclass(frozen=True)
class Credential:
    """A clone- or push-scoped credential, held by the platform, never the sandbox.

    The secret is excluded from ``repr`` so a logged credential leaks nothing.
    """

    origin: str
    header: CredentialHeader
    secret: str = field(repr=False)
    scope: CredentialScope
    expiry: CredentialExpiry
    expires_at: datetime | None = None
    username: str | None = None

    def __post_init__(self) -> None:
        _require_text("Credential", origin=self.origin, secret=self.secret)
        if (self.expiry is CredentialExpiry.KNOWN) != (self.expires_at is not None):
            raise ValueError("Credential.expires_at is set exactly when the expiry is known")
        if self.header is CredentialHeader.AUTHORIZATION_BASIC and not self.username:
            raise ValueError("a basic credential needs a username")

    def git_header(self) -> str:
        """The ``http.extraHeader`` value git sends to ``origin``."""

        if self.header is CredentialHeader.AUTHORIZATION_BASIC:
            pair = f"{self.username}:{self.secret}".encode()
            return f"Authorization: Basic {base64.b64encode(pair).decode()}"
        if self.header is CredentialHeader.AUTHORIZATION_BEARER:
            return f"Authorization: Bearer {self.secret}"
        return f"PRIVATE-TOKEN: {self.secret}"


class CheckState(enum.StrEnum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILURE = "failure"
    NEUTRAL = "neutral"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class NormalizedCheck:
    """One CI check on one exact commit. ``key`` is the stable, configurable
    check identity (ADR 0197 consequence 7); ``name`` is for display."""

    key: str
    state: CheckState
    head_sha: str
    name: str
    url: str | None = None


class RollupState(enum.StrEnum):
    NONE = "none"
    PENDING = "pending"
    SUCCESS = "success"
    FAILURE = "failure"


# Cancelled counts as failing, as GitHub's cancelled conclusion always has here.
_FAILING = frozenset({CheckState.FAILURE, CheckState.CANCELLED})


@dataclass(frozen=True)
class CiRollup:
    """CI on exactly one head commit. Build it with :meth:`on_head`."""

    head_sha: str
    state: RollupState
    checks: tuple[NormalizedCheck, ...]

    @classmethod
    def on_head(cls, head_sha: str, checks: tuple[NormalizedCheck, ...]) -> CiRollup:
        """Roll up only the checks reported for ``head_sha``.

        A check for any other commit is dropped, so a stale green run on an
        older head can never pass a newer one. A failure outranks a pending
        check, matching the GitHub verdict the factory used before the port.
        """

        own = tuple(check for check in checks if check.head_sha == head_sha)
        if not own:
            state = RollupState.NONE
        elif any(check.state in _FAILING for check in own):
            state = RollupState.FAILURE
        elif any(check.state is CheckState.PENDING for check in own):
            state = RollupState.PENDING
        else:
            state = RollupState.SUCCESS
        return cls(head_sha=head_sha, state=state, checks=own)


@dataclass(frozen=True)
class CiDiagnostic:
    """A failing check's log excerpt, from the optional ``ci_diagnostics``."""

    check_key: str
    excerpt: str


class Disposition(enum.StrEnum):
    ADMIT = "admit"
    CANCEL = "cancel"
    MENTION = "mention"


@dataclass(frozen=True)
class MarkedNotice:
    """One marking on a tracker issue: a label applied or removed, or a mention.

    ``event_id`` is the tracker's immutable id for the event (a GitHub timeline
    event id, a webhook delivery id, or a comment id for a mention). ``cursor``
    is the opaque poll position just after this notice.
    """

    issue: TrackerIssueRef
    marker: str
    actor: Actor
    event_id: str
    disposition: Disposition
    cursor: str


@dataclass(frozen=True)
class NoticePage:
    """A complete poll since a cursor and the cursor to poll from next."""

    notices: tuple[MarkedNotice, ...]
    cursor: str


class FeedbackKind(enum.StrEnum):
    COMMENT = "comment"
    REVIEW = "review"
    REVIEW_COMMENT = "review_comment"


@dataclass(frozen=True)
class ReviewFeedback:
    """One human comment, review, or review comment on a pull request."""

    id: str
    author: Actor
    body: str
    pull_request: PullRequestRef
    head_sha: str
    kind: FeedbackKind
    thread_id: str | None = None


@dataclass(frozen=True)
class FeedbackPage:
    items: tuple[ReviewFeedback, ...]
    cursor: str


@dataclass(frozen=True)
class MarkedComment:
    """A status comment the adapter's own identity wrote, carrying a marker."""

    id: str
    target: ReplyTarget
    body: str


@dataclass(frozen=True)
class UpsertResult:
    comment: MarkedComment
    written: bool
