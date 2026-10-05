"""In-memory Tracker, CodeHost and MarkedComments adapters.

They are real implementations of the ports over dict state, run by the same
contract suite as every forge adapter, and seeded through the ``seed_*`` and
``grant`` methods below rather than through private fields. They model
pagination (a page size and injectable page failures), comment ownership by
identity, write permission, labels with timeline events, pull requests whose
head moves, CI checks per commit, and review feedback.

Every port call that changes forge state counts one write in a shared
`WriteLedger`, so a test can prove a no-op wrote nothing.
"""

from __future__ import annotations

import itertools
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace

from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    MARKED_COMMENT_OPERATIONS,
    TRACKER_OPERATIONS,
    Operation,
    Support,
    validate_declaration,
)
from curie_api.forges.errors import NotFound, Unavailable, Unsupported
from curie_api.forges.ports import CodeHost, MarkedComments, Tracker
from curie_api.forges.types import (
    Actor,
    CheckState,
    CiDiagnostic,
    CiRollup,
    Commit,
    Credential,
    CredentialExpiry,
    CredentialHeader,
    CredentialScope,
    Disposition,
    FeedbackKind,
    FeedbackPage,
    MarkedComment,
    MarkedNotice,
    NormalizedCheck,
    NoticePage,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    ReviewFeedback,
    TrackerIssueRef,
    UpsertResult,
)

BOT = Actor(id="1000", login="curie-bot")


def all_supported(operations: frozenset[Operation]) -> dict[Operation, Support]:
    return dict.fromkeys(operations, Support.SUPPORTED)


class WriteLedger:
    """Counts forge-state writes made through port calls (not through seeding)."""

    def __init__(self) -> None:
        self.writes = 0

    def record(self) -> None:
        self.writes += 1


class PageFaults:
    """Injectable listing failures: the read after ``after`` more page reads fails once."""

    def __init__(self) -> None:
        self._countdown: int | None = None

    def fail_next_page(self, *, after: int = 0) -> None:
        self._countdown = after

    def read_page(self, what: str) -> None:
        if self._countdown is None:
            return
        if self._countdown == 0:
            self._countdown = None
            raise Unavailable(what)
        self._countdown -= 1


def _declared(
    capabilities: Mapping[Operation, Support] | None, operations: frozenset[Operation]
) -> dict[Operation, Support]:
    declared = all_supported(operations) if capabilities is None else dict(capabilities)
    validate_declaration(declared, operations)
    return declared


def _cursor_position(cursor: str | None) -> int:
    if cursor is None:
        return 0
    if not cursor.isdigit():
        raise ValueError("an in-memory cursor is a decimal position")
    return int(cursor)


class _Declared:
    """Capability enforcement shared by the in-memory adapters."""

    capabilities: Mapping[Operation, Support]

    def _gate(self, operation: Operation) -> bool:
        """True when the operation should run; False when it is a declared no-op."""

        support = self.capabilities[operation]
        if support is Support.UNSUPPORTED:
            raise Unsupported(operation)
        return support is Support.SUPPORTED


@dataclass
class _Comment:
    id: str
    author: Actor
    body: str


class InMemoryMarkedComments(_Declared):
    """Marked comments on one side: tracker issues, or pull requests and threads."""

    def __init__(
        self,
        *,
        kind: str,
        host: str,
        identity: Actor,
        accepts: Collection[str],
        ledger: WriteLedger,
        faults: PageFaults,
        page_size: int,
        capabilities: Mapping[Operation, Support] | None = None,
    ) -> None:
        self.kind = kind
        self.host = host
        self.identity = identity
        self.capabilities = _declared(capabilities, MARKED_COMMENT_OPERATIONS)
        self._accepts = frozenset(accepts)
        self._ledger = ledger
        self._faults = faults
        self._page_size = page_size
        self._threads: dict[ReplyTarget, list[_Comment]] = {}
        self._ids = itertools.count(1)

    @staticmethod
    def embed(marker: str, body: str) -> str:
        """How this adapter carries the core-owned marker inside a comment."""

        return f"{body}\n\n<!-- {marker} -->"

    def _accept(self, target: ReplyTarget) -> list[_Comment]:
        if target.kind not in self._accepts:
            raise ValueError(f"{target.kind} comments live on the other port")
        return self._threads.setdefault(target, [])

    def seed_comment(self, target: ReplyTarget, author: Actor, body: str) -> str:
        """A comment someone else (or a past run) left; returns its id."""

        comment = _Comment(f"c{next(self._ids)}", author, body)
        self._accept(target).append(comment)
        return comment.id

    def comments(self, target: ReplyTarget) -> list[tuple[Actor, str]]:
        return [(comment.author, comment.body) for comment in self._accept(target)]

    async def find_marked(self, target: ReplyTarget, marker: str) -> MarkedComment | None:
        self._gate(Operation.FIND_MARKED)
        thread = self._accept(target)
        embedded = f"<!-- {marker} -->"
        for start in range(0, max(len(thread), 1), self._page_size):
            self._faults.read_page("comments")
            for comment in thread[start : start + self._page_size]:
                # Only our own identity's comments count; a pasted marker does not.
                if comment.author == self.identity and embedded in comment.body:
                    return MarkedComment(comment.id, target, comment.body)
        return None

    async def upsert_marked(self, target: ReplyTarget, marker: str, body: str) -> UpsertResult:
        self._gate(Operation.UPSERT_MARKED)
        text = self.embed(marker, body)
        found = await self.find_marked(target, marker)
        thread = self._accept(target)
        if found is not None:
            comment = next(item for item in thread if item.id == found.id)
            if comment.body == text:
                return UpsertResult(found, written=False)
            comment.body = text
            self._ledger.record()
            return UpsertResult(MarkedComment(comment.id, target, text), written=True)
        comment_id = self.seed_comment(target, self.identity, text)
        self._ledger.record()
        return UpsertResult(MarkedComment(comment_id, target, text), written=True)


@dataclass
class _Issue:
    ref: TrackerIssueRef
    title: str
    body: str
    labels: set[str] = field(default_factory=set)
    dependencies: tuple[TrackerIssueRef, ...] = ()
    linked: list[PullRequestRef] = field(default_factory=list)


@dataclass(frozen=True)
class _TimelineEvent:
    position: int
    event_id: str
    issue: TrackerIssueRef
    disposition: Disposition
    marker: str
    actor: Actor


class InMemoryTracker(_Declared):
    """A tracker over dict state.

    A native kind authorizes starts by repository write access; a tracker-only
    kind (`types.MEMORY_TRACKER_ONLY`) by the binding's allowlist. Both are the
    single ``grant`` set here, which is what the harness drives.
    """

    def __init__(
        self,
        *,
        kind: str = types.MEMORY,
        host: str = "tracker.memory.example",
        scope_id: str = "7001",
        label: str = "factory",
        mention: str = "curie",
        identity: Actor = BOT,
        page_size: int = 2,
        capabilities: Mapping[Operation, Support] | None = None,
        comment_capabilities: Mapping[Operation, Support] | None = None,
        ledger: WriteLedger | None = None,
        faults: PageFaults | None = None,
    ) -> None:
        self.kind = kind
        self.host = host
        self.scope_id = scope_id
        self.label = label
        self.mention = mention
        self.identity = identity
        self.capabilities = _declared(capabilities, TRACKER_OPERATIONS)
        self.ledger = ledger or WriteLedger()
        self.faults = faults or PageFaults()
        self._page_size = page_size
        self._issues: dict[TrackerIssueRef, _Issue] = {}
        self._timeline: list[_TimelineEvent] = []
        self._authorized: set[str] = set()
        self._ids = itertools.count(9001)
        self._comments = InMemoryMarkedComments(
            kind=kind,
            host=host,
            identity=identity,
            accepts={"issue"},
            ledger=self.ledger,
            faults=self.faults,
            page_size=page_size,
            capabilities=comment_capabilities,
        )

    @property
    def marked_comments(self) -> InMemoryMarkedComments:
        return self._comments

    # Seeding --------------------------------------------------------------

    def seed_issue(
        self, title: str, body: str, *, display_key: str | None = None
    ) -> TrackerIssueRef:
        ref = TrackerIssueRef(
            self.kind, self.host, self.scope_id, str(len(self._issues) + 1), display_key
        )
        self._issues[ref] = _Issue(ref, title, body)
        return ref

    def seed_dependencies(self, issue: TrackerIssueRef, *depends_on: TrackerIssueRef) -> None:
        self._issue(issue).dependencies = depends_on

    def grant(self, actor: Actor, allowed: bool = True) -> None:
        (self._authorized.add if allowed else self._authorized.discard)(actor.id)

    def _record(
        self, issue: TrackerIssueRef, disposition: Disposition, marker: str, actor: Actor
    ) -> str:
        event = _TimelineEvent(
            position=len(self._timeline) + 1,
            event_id=str(next(self._ids)),
            issue=issue,
            disposition=disposition,
            marker=marker,
            actor=actor,
        )
        self._timeline.append(event)
        return event.event_id

    def apply_label(self, issue: TrackerIssueRef, label: str, actor: Actor) -> str:
        self._issue(issue).labels.add(label)
        return self._record(issue, Disposition.ADMIT, label, actor)

    def remove_label(self, issue: TrackerIssueRef, label: str, actor: Actor) -> str:
        self._issue(issue).labels.discard(label)
        return self._record(issue, Disposition.CANCEL, label, actor)

    def mention_in_comment(self, issue: TrackerIssueRef, actor: Actor) -> str:
        self._issue(issue)
        return self._record(issue, Disposition.MENTION, f"@{self.mention}", actor)

    def labels(self, issue: TrackerIssueRef) -> frozenset[str]:
        return frozenset(self._issue(issue).labels)

    def linked(self, issue: TrackerIssueRef) -> tuple[PullRequestRef, ...]:
        return tuple(self._issue(issue).linked)

    def _issue(self, issue: TrackerIssueRef) -> _Issue:
        found = self._issues.get(issue)
        if found is None:
            raise NotFound("issue")
        return found

    # Port -----------------------------------------------------------------

    def _notice(self, event: _TimelineEvent) -> MarkedNotice:
        return MarkedNotice(
            issue=event.issue,
            marker=event.marker,
            actor=event.actor,
            event_id=event.event_id,
            disposition=event.disposition,
            cursor=str(event.position),
        )

    def _marks(self, event: _TimelineEvent) -> bool:
        if event.actor == self.identity:
            return False
        if event.disposition is Disposition.MENTION:
            return True
        return event.marker == self.label

    async def poll_marked(
        self, cursor: str | None, *, running: Sequence[TrackerIssueRef]
    ) -> NoticePage:
        self._gate(Operation.POLL_MARKED)
        start = _cursor_position(cursor)
        unread = self._timeline[start:]
        notices: list[MarkedNotice] = []
        for offset in range(0, len(unread), self._page_size):
            # Read every page before returning any notice or cursor.
            self.faults.read_page("timeline")
            notices.extend(
                self._notice(event)
                for event in unread[offset : offset + self._page_size]
                if self._marks(event)
            )
        if not unread:
            self.faults.read_page("timeline")
        seen = {(notice.issue, notice.event_id) for notice in notices}
        for issue in running:
            # A running WorkItem is re-read: a marker gone before the cursor
            # still cancels it.
            current = self._issues.get(issue)
            if current is None or self.label in current.labels:
                continue
            removal = self._last(issue, Disposition.CANCEL)
            if removal is not None and (issue, removal.event_id) not in seen:
                notices.append(self._notice(removal))
        return NoticePage(tuple(notices), str(len(self._timeline)))

    def _last(self, issue: TrackerIssueRef, disposition: Disposition) -> _TimelineEvent | None:
        for event in reversed(self._timeline):
            if (
                event.issue == issue
                and event.disposition is disposition
                and event.marker == self.label
                and event.actor != self.identity
            ):
                return event
        return None

    async def verify_current(self, notice: MarkedNotice) -> bool:
        self._gate(Operation.VERIFY_CURRENT)
        issue = self._issues.get(notice.issue)
        if issue is None:
            return False
        if notice.disposition is Disposition.MENTION:
            return any(event.event_id == notice.event_id for event in self._timeline)
        if notice.disposition is Disposition.CANCEL:
            return self.label not in issue.labels
        latest = self._last(notice.issue, Disposition.ADMIT)
        return (
            self.label in issue.labels and latest is not None and latest.event_id == notice.event_id
        )

    async def marking_actor(self, issue: TrackerIssueRef, marker: str) -> Actor | None:
        self._gate(Operation.MARKING_ACTOR)
        if marker not in self._issue(issue).labels:
            return None
        for event in reversed(self._timeline):
            if (
                event.issue == issue
                and event.disposition is Disposition.ADMIT
                and event.marker == marker
            ):
                return event.actor
        return None

    async def may_start(self, issue: TrackerIssueRef, actor: Actor) -> bool:
        self._gate(Operation.MAY_START)
        self._issue(issue)
        return actor.id in self._authorized and actor != self.identity

    async def read_ticket(self, issue: TrackerIssueRef) -> str:
        self._gate(Operation.READ_TICKET)
        found = self._issue(issue)
        return f"# {found.title}\n\n{found.body}"

    async def set_state_label(
        self, issue: TrackerIssueRef, *, add: str | None, remove: Collection[str]
    ) -> None:
        self._gate(Operation.SET_STATE_LABEL)
        found = self._issue(issue)
        for label in remove:
            if label in found.labels:
                self.remove_label(issue, label, self.identity)
                self.ledger.record()
        if add is not None and add not in found.labels:
            self.apply_label(issue, add, self.identity)
            self.ledger.record()

    async def closing_reference(self, issue: TrackerIssueRef, repository: RepositoryRef) -> str:
        self._gate(Operation.CLOSING_REFERENCE)
        found = self._issue(issue)
        return f"Closes {found.ref.display_key or '#' + found.ref.issue_id}"

    async def link_pull_request(self, issue: TrackerIssueRef, pull_request: PullRequest) -> None:
        if not self._gate(Operation.LINK_PULL_REQUEST):
            return
        self._issue(issue).linked.append(pull_request.ref)
        self.ledger.record()

    async def dependencies(self, issue: TrackerIssueRef) -> tuple[TrackerIssueRef, ...]:
        if not self._gate(Operation.DEPENDENCIES):
            return ()
        return self._issue(issue).dependencies


@dataclass
class _PullRequest:
    ref: PullRequestRef
    head_ref: str
    base_ref: str
    state: PullRequestState
    title: str
    body: str


@dataclass(frozen=True)
class _Feedback:
    position: int
    feedback: ReviewFeedback


class InMemoryCodeHost(_Declared):
    """A code host over dict state with one or more repositories."""

    def __init__(
        self,
        *,
        kind: str = types.MEMORY,
        host: str = "code.memory.example",
        identity: Actor = BOT,
        page_size: int = 2,
        capabilities: Mapping[Operation, Support] | None = None,
        comment_capabilities: Mapping[Operation, Support] | None = None,
        ledger: WriteLedger | None = None,
        faults: PageFaults | None = None,
    ) -> None:
        self.kind = kind
        self.host = host
        self.identity = identity
        self.capabilities = _declared(capabilities, CODE_HOST_OPERATIONS)
        self.ledger = ledger or WriteLedger()
        self.faults = faults or PageFaults()
        self._page_size = page_size
        self._repositories: dict[str, RepositoryRef] = {}
        self._branches: dict[tuple[RepositoryRef, str], str] = {}
        self._commits: dict[str, Commit] = {}
        self._pulls: dict[PullRequestRef, _PullRequest] = {}
        self._checks: dict[str, dict[str, NormalizedCheck]] = {}
        self._excerpts: dict[tuple[str, str], str] = {}
        self._feedback: dict[PullRequestRef, list[_Feedback]] = {}
        self._writers: dict[RepositoryRef, set[str]] = {}
        self._shas = itertools.count(1)
        self._ids = itertools.count(5001)
        self._comments = InMemoryMarkedComments(
            kind=kind,
            host=host,
            identity=identity,
            accepts={"pull_request", "review_thread"},
            ledger=self.ledger,
            faults=self.faults,
            page_size=page_size,
            capabilities=comment_capabilities,
        )

    @property
    def marked_comments(self) -> InMemoryMarkedComments:
        return self._comments

    # Seeding --------------------------------------------------------------

    def seed_repository(self, path: str, *, project_id: str | None = None) -> RepositoryRef:
        ref = RepositoryRef(
            self.kind, self.host, project_id or str(len(self._repositories) + 3001), path
        )
        self._repositories[ref.project_id] = ref
        self._writers.setdefault(ref, set())
        return ref

    def rename_repository(self, repository: RepositoryRef, path: str) -> None:
        self._repositories[repository.project_id] = replace(repository, path=path)

    def grant(self, repository: RepositoryRef, actor: Actor, allowed: bool = True) -> None:
        writers = self._writers[repository]
        (writers.add if allowed else writers.discard)(actor.id)

    def push(self, repository: RepositoryRef, branch: str, message: str = "change") -> str:
        """A new commit on ``branch``; returns its sha."""

        parent = self._branches.get((repository, branch))
        sha = f"{next(self._shas):040x}"
        self._commits[sha] = Commit(sha, (parent,) if parent else (), message)
        self._branches[(repository, branch)] = sha
        return sha

    def report_checks(
        self, sha: str, checks: Mapping[str, CheckState], *, excerpt: str | None = None
    ) -> None:
        reported = self._checks.setdefault(sha, {})
        for key, state in checks.items():
            reported[key] = NormalizedCheck(
                key, state, sha, key.title(), f"https://{self.host}/ci/{sha[:12]}/{key}"
            )
            if excerpt is not None and state in {CheckState.FAILURE, CheckState.CANCELLED}:
                self._excerpts[(sha, key)] = excerpt

    def seed_feedback(
        self,
        pull_request: PullRequestRef,
        author: Actor,
        body: str,
        kind: FeedbackKind,
        *,
        thread_id: str | None = None,
    ) -> ReviewFeedback:
        current = self._pull(pull_request)
        head = self._branches[(pull_request.repository, current.head_ref)]
        items = self._feedback.setdefault(pull_request, [])
        feedback = ReviewFeedback(
            id=str(next(self._ids)),
            author=author,
            body=body,
            pull_request=pull_request,
            head_sha=head,
            kind=kind,
            thread_id=thread_id,
        )
        items.append(_Feedback(len(items) + 1, feedback))
        return feedback

    def delete_feedback(self, feedback: ReviewFeedback) -> None:
        items = self._feedback.get(feedback.pull_request, [])
        self._feedback[feedback.pull_request] = [
            item for item in items if item.feedback.id != feedback.id
        ]

    def set_state(self, pull_request: PullRequestRef, state: PullRequestState) -> None:
        self._pull(pull_request).state = state

    def check_states(self, sha: str) -> dict[str, CheckState]:
        return {key: check.state for key, check in self._checks.get(sha, {}).items()}

    def _repository(self, repository: RepositoryRef) -> RepositoryRef:
        if repository.project_id not in self._repositories:
            raise NotFound("repository")
        return self._repositories[repository.project_id]

    def _pull(self, pull_request: PullRequestRef) -> _PullRequest:
        found = self._pulls.get(pull_request)
        if found is None:
            raise NotFound("pull_request")
        return found

    def _view(self, pull: _PullRequest) -> PullRequest:
        repository = self._repositories[pull.ref.repository.project_id]
        return PullRequest(
            ref=pull.ref,
            head_sha=self._branches[(pull.ref.repository, pull.head_ref)],
            head_ref=pull.head_ref,
            base_ref=pull.base_ref,
            state=pull.state,
            url=f"https://{self.host}/{repository.path}/pull/{pull.ref.number}",
            title=pull.title,
            body=pull.body,
        )

    # Port -----------------------------------------------------------------

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        self._gate(Operation.RESOLVE_REPOSITORY)
        found = self._repositories.get(project_id)
        if found is None:
            raise NotFound("repository")
        return found

    async def credential(self, repository: RepositoryRef, scope: CredentialScope) -> Credential:
        self._gate(Operation.CREDENTIAL)
        current = self._repository(repository)
        return Credential(
            origin=f"https://{self.host}/{current.path}.git",
            header=CredentialHeader.AUTHORIZATION_BEARER,
            secret=f"memory-{scope}-{current.project_id}",
            scope=scope,
            expiry=CredentialExpiry.UNKNOWN,
        )

    async def branch_head(self, repository: RepositoryRef, branch: str) -> str | None:
        self._gate(Operation.BRANCH_HEAD)
        self._repository(repository)
        return self._branches.get((repository, branch))

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit:
        self._gate(Operation.READ_COMMIT)
        self._repository(repository)
        found = self._commits.get(sha)
        if found is None:
            raise NotFound("commit")
        return found

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        self._gate(Operation.FIND_PULL_REQUEST)
        self._repository(repository)
        matches = [
            pull
            for pull in self._pulls.values()
            if pull.ref.repository == repository and pull.head_ref == head_ref
        ]
        return self._view(matches[-1]) if matches else None

    async def open_pull_request(
        self,
        repository: RepositoryRef,
        *,
        head_ref: str,
        base_ref: str,
        title: str,
        body: str,
    ) -> PullRequest:
        self._gate(Operation.OPEN_PULL_REQUEST)
        self._repository(repository)
        if (repository, head_ref) not in self._branches or (
            repository,
            base_ref,
        ) not in self._branches:
            raise NotFound("branch")
        ref = PullRequestRef(repository, str(len(self._pulls) + 1))
        self._pulls[ref] = _PullRequest(ref, head_ref, base_ref, PullRequestState.OPEN, title, body)
        self.ledger.record()
        return self._view(self._pulls[ref])

    async def update_pull_request(
        self, pull_request: PullRequestRef, *, title: str | None, body: str | None
    ) -> PullRequest:
        self._gate(Operation.UPDATE_PULL_REQUEST)
        pull = self._pull(pull_request)
        if title is not None:
            pull.title = title
        if body is not None:
            pull.body = body
        self.ledger.record()
        return self._view(pull)

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest:
        self._gate(Operation.READ_PULL_REQUEST)
        return self._view(self._pull(pull_request))

    async def observe_ci(self, repository: RepositoryRef, head_sha: str) -> CiRollup:
        self._gate(Operation.OBSERVE_CI)
        self._repository(repository)
        return CiRollup.on_head(head_sha, tuple(self._checks.get(head_sha, {}).values()))

    async def list_review_feedback(
        self, pull_request: PullRequestRef, cursor: str | None
    ) -> FeedbackPage:
        self._gate(Operation.LIST_REVIEW_FEEDBACK)
        self._pull(pull_request)
        start = _cursor_position(cursor)
        unread = [item for item in self._feedback.get(pull_request, []) if item.position > start]
        listed: list[ReviewFeedback] = []
        for offset in range(0, max(len(unread), 1), self._page_size):
            self.faults.read_page("feedback")
            listed.extend(
                item.feedback
                for item in unread[offset : offset + self._page_size]
                if item.feedback.author != self.identity
            )
        position = unread[-1].position if unread else start
        return FeedbackPage(tuple(listed), str(position))

    async def verify_feedback(self, feedback: ReviewFeedback) -> bool:
        self._gate(Operation.VERIFY_FEEDBACK)
        pull = self._pulls.get(feedback.pull_request)
        if pull is None or pull.state is not PullRequestState.OPEN:
            return False
        return any(item.feedback == feedback for item in self._feedback.get(pull.ref, []))

    async def ci_diagnostics(
        self, repository: RepositoryRef, head_sha: str
    ) -> tuple[CiDiagnostic, ...]:
        self._gate(Operation.CI_DIAGNOSTICS)
        self._repository(repository)
        return tuple(
            CiDiagnostic(key, excerpt)
            for (sha, key), excerpt in self._excerpts.items()
            if sha == head_sha
        )

    async def rerun_failed(self, repository: RepositoryRef, head_sha: str) -> int:
        if not self._gate(Operation.RERUN_FAILED):
            return 0
        self._repository(repository)
        reported = self._checks.get(head_sha, {})
        failed = [
            key
            for key, check in reported.items()
            if check.state in {CheckState.FAILURE, CheckState.CANCELLED}
        ]
        for key in failed:
            reported[key] = replace(reported[key], state=CheckState.PENDING)
        if failed:
            self.ledger.record()
        return len(failed)

    async def user_can_write(self, repository: RepositoryRef, actor: Actor) -> bool:
        self._gate(Operation.USER_CAN_WRITE)
        return actor.id in self._writers.get(self._repository(repository), set())


def _conforms(
    tracker: InMemoryTracker, code_host: InMemoryCodeHost
) -> tuple[Tracker, CodeHost, MarkedComments]:
    """Type-checked proof that the in-memory adapters satisfy the ports."""

    return tracker, code_host, tracker.marked_comments
