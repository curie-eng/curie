"""Forge-neutral values that cross the Tracker and CodeHost ports (ADR 0197).

Nothing here names a GitHub field. Every identifier is text, because GitHub
numbers, GitLab iids, Jira numeric ids and Bitbucket ids do not share a type.
Equality follows the ADR's identity rules: a display key or a path is carried
for display and never takes part in comparison.
"""

from __future__ import annotations

import base64
import enum
from collections.abc import Awaitable, Callable
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
    # Filled by `resolve_repository`; None when the reference was built from
    # stored facts without a read. Never part of identity.
    default_branch: str | None = field(default=None, compare=False)

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
    draft: bool = False
    # When the code host last changed the pull request, as it reports it; None
    # when the payload carried no usable time. A metadata-only revision records
    # it so CI freshness is judged after the edit.
    updated_at: datetime | None = None


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


class CheckSource(enum.StrEnum):
    """What reported a check: a check or job run, or a commit status an
    external system posted against the commit."""

    RUN = "run"
    STATUS = "status"


@dataclass(frozen=True)
class NormalizedCheck:
    """One CI check on one exact commit. ``key`` is the stable, configurable
    check identity (ADR 0197 consequence 7); ``name`` is for display.

    ``reported_state`` is the code host's own word for the state (a conclusion
    such as ``timed_out``, or a run status such as ``queued``), reported to
    people and agents as the host said it; it defaults to ``state``.
    ``started_at`` is when this run started, or when the status was posted.
    ``check_id`` is the host's id for this run of the check, so a rerun's new
    attempt can be told from the failure it replaces. ``native`` marks a check
    run by the code host's own CI, the only kind ``rerun_failed`` can rerun.
    """

    key: str
    state: CheckState
    head_sha: str
    name: str
    url: str | None = None
    source: CheckSource = CheckSource.RUN
    reported_state: str = ""
    started_at: datetime | None = None
    check_id: str | None = None
    native: bool = False

    def __post_init__(self) -> None:
        if not self.reported_state:
            object.__setattr__(self, "reported_state", self.state.value)


# Normalized check keys (ADR 0197 consequence 7): a check run is keyed by its
# name and a commit status by ``status:<context>``, so the two never collide.
STATUS_KEY_PREFIX = "status:"
_ESCAPED_CHECK_PREFIX = "check:"


def check_run_key(name: str) -> str:
    """The normalized key of a check run: its name.

    A name that already starts with ``status:`` or ``check:`` is escaped with
    ``check:``, so no check run key can equal a commit status key.
    """

    if name.startswith((STATUS_KEY_PREFIX, _ESCAPED_CHECK_PREFIX)):
        return f"{_ESCAPED_CHECK_PREFIX}{name}"
    return name


def status_key(context: str) -> str:
    """The normalized key of a commit status: ``status:<context>``."""

    return f"{STATUS_KEY_PREFIX}{context}"


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

    @property
    def failing_keys(self) -> frozenset[str]:
        """The keys of the checks failing on this head."""

        return frozenset(check.key for check in self.checks if check.state in _FAILING)


@dataclass(frozen=True)
class CiAnnotation:
    """One line a failing check pointed at."""

    path: str | None
    line: int | None
    message: str | None


@dataclass(frozen=True)
class CiDiagnostic:
    """What one failing check said, from the optional ``ci_diagnostics``.

    Every part is redacted. ``excerpt`` joins them, newest text last.
    ``check_id`` is the failing check's `NormalizedCheck.check_id`. ``log`` is
    the tail of its log; ``log_unavailable`` says a log exists but could not
    be read now.
    """

    check_key: str
    excerpt: str
    check_id: str | None = None
    title: str | None = None
    summary: str | None = None
    annotations: tuple[CiAnnotation, ...] = ()
    log: str | None = None
    log_unavailable: bool = False


@dataclass(frozen=True)
class CiReport:
    """The checks on one head and the diagnostics of its failing ones, read once.

    ``base`` is the checks on the current head of the base branch the caller
    named, read only when a head check fails (#4105). It is None when it was
    not read or could not be; it never fails the head's report.
    """

    rollup: CiRollup
    diagnostics: tuple[CiDiagnostic, ...]
    base: CiRollup | None = None


@dataclass(frozen=True)
class RerunJob:
    """A failed native check run to rerun, and the rerun unit once known.

    ``unit`` is what the code host reruns as one request (a workflow run on
    GitHub); several jobs may share one. A caller that already knows it passes
    it back so the host is not asked again.
    """

    check_id: str
    name: str
    url: str | None = None
    unit: str | None = None

    @classmethod
    def of(cls, check: NormalizedCheck) -> RerunJob:
        assert check.check_id is not None
        return cls(check.check_id, check.name, check.url)


def failed_native_jobs(checks: tuple[NormalizedCheck, ...]) -> tuple[RerunJob, ...]:
    """The failed or cancelled native check runs, in order, once each."""

    jobs: dict[str, RerunJob] = {}
    for check in checks:
        if (
            check.native
            and check.source is CheckSource.RUN
            and check.state in _FAILING
            and check.check_id is not None
            and check.check_id not in jobs
        ):
            jobs[check.check_id] = RerunJob.of(check)
    return tuple(jobs.values())


class RerunOutcome(enum.StrEnum):
    """The code host's answer to one rerun request."""

    ACCEPTED = "accepted"
    # A definitive refusal; asking again will not change it.
    REFUSED = "refused"
    # Transport, rate limit or a server error; nothing was rerun.
    RETRY = "retry"
    # Sent, but the answer was lost; it may have been accepted, so it is not
    # sent again.
    UNCONFIRMED = "unconfirmed"


@dataclass(frozen=True)
class RerunAttempt:
    unit: str
    outcome: RerunOutcome
    reason: str | None = None


@dataclass(frozen=True)
class RerunRecord:
    """What one ``rerun_failed`` call did, unit by unit.

    ``jobs`` are the jobs asked for, each with its resolved ``unit``.
    ``attempts`` holds one answer per unit asked, in order; a ``RETRY`` or
    ``UNCONFIRMED`` answer ends the call. ``stopped`` is set when the call
    ended before any unit could be asked (no credential, or a unit that could
    not be resolved), with its ``reason``.
    """

    jobs: tuple[RerunJob, ...]
    attempts: tuple[RerunAttempt, ...] = ()
    stopped: RerunOutcome | None = None
    reason: str | None = None


RerunObserver = Callable[[RerunRecord], Awaitable[bool]]


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
