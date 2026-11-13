"""Deliver one tracker result exactly once and record its receipt (ADR 0277 decision 4, #5585).

Inactive in #5585: no router, lifespan task, reconciler, worker path or CLI
imports this module, and only tests insert grants. #5586 mints grants, calls
`submit_result` from the runner result tool and schedules `deliver` and
`expire_overdue`; #5588 reads receipts. This docstring is the frozen contract
those stages build on.

Grant (`models.FactoryTrackerResultGrant`). The authority one execution
request holds to deliver one result: ``work_item_id``,
``execution_request_id`` (unique), ``source_fingerprint`` (`source_fingerprint`
at minting), ``repository_id`` (the WorkItem's ``repository_project_id``),
``allowed_result_kinds`` (sorted JSON list, ``["findings"]`` or
``["findings", "ticket"]``), ``output_identity`` (unique;
`output_identity_for`), ``implementation_label`` (copied from the
declaration; set exactly when a ticket is allowed) and ``expires_at``.

Proposal. `curie_api.factory_tracker_results` (``ticket`` or ``findings``).
It names no destination: the source issue is the grant's WorkItem's issue and
the output issue is created in that issue's own scope.

Output identity and markers. `output_identity_for` is
``execution-request:<request id>``; the linked issue carries the tracker's
embedding of ``curie-linked-issue:<output identity>`` as its last line
(`curie_api.forges.types.linked_issue_marker`). The source issue's result
comment carries `result_marker`, ``curie-tracker-result:<request id>``, one per
source execution request and distinct from the status comment marker
(`curie_api.factory_comment_text.marker_for`). Model text cannot plant any of
them: every rendered text has ``<!--`` written as ``&lt;!--``, after
`redact_factory_comment`. Nor can it mention anyone: every ``@`` in it is
followed by U+200B, so a model-written ``@login`` never notifies that account.

Fences, checked in this order at submission, again immediately before every
tracker write of an attempt, and in the transaction that records delivery
(see Rechecks); a failure writes nothing more to the tracker (`RefusalReason`):

1. The grant exists (``grant_not_found``), names the presented execution
   request (``grant_request_mismatch``), and names that request's WorkItem and
   repository (``grant_scope_mismatch``). Submission only: the grant has not
   expired (``grant_expired``) and allows the proposal's kind
   (``result_kind_not_allowed``).
2. ``source_fingerprint`` still matches (``source_fingerprint_changed``).
3. Neither the WorkItem nor the request is cancelled (``cancelled``).
4. A live owner (``owner_not_live``): the request is running under the
   presented runtime epoch (sealed on the delivery for attempts), with a
   runtime owner, a current heartbeat lease and an unexpired execution
   deadline.
5. The tracker is the WorkItem's (``tracker_mismatch``) and, for a ticket,
   supports `Operation.CREATE_LINKED_ISSUE` (``tracker_operation_unsupported``).
6. Submission only: the ticket title or body carries no closing keyword
   followed by a reference to the source (``closes_source``).

A duplicate submission for one grant returns the existing delivery when its
proposal digest matches and refuses ``proposal_conflict`` otherwise.

State machine (``factory_tracker_result_deliveries.state``). ``owed`` moves
to ``delivered`` only after both read-backs pass (the result comment exists
under `result_marker` with the upserted id and, for a ticket, links the output
issue, and the output issue's read-back body digest equals the digest of the
body sent), or to ``failed`` with a ``failure_code``: ``delivery_deadline_exceeded``
(`FailureCode`), ``output_ambiguous``, ``tracker_operation_unsupported``, or
any fence reason that fails at attempt time (fences do not heal). Nothing
leaves ``delivered`` or ``failed``. An attempt that hits a transient error
(`AttemptError`: ``tracker_unavailable``, ``tracker_unauthorized``,
``tracker_not_found``, ``readback_mismatch``, ``output_mismatch``,
``output_missing``, ``output_unconfirmed``) records it
as ``last_error_code``, backs off ``next_attempt_at`` with the WorkItem
backoff settings and stays owed until the deadline,
``Settings.factory_tracker_result_delivery_deadline_seconds`` (default 900)
after submission. The attempt claim itself refuses a row whose
``next_attempt_at`` is still ahead (``attempted`` False, nothing written), so
retry eligibility holds for every caller of `deliver`, not only for a
scheduler that picks rows with `due_deliveries`. A row past its deadline is
claimable at any time: that attempt only settles it failed, with no tracker
call.

Exactly once. An attempt holds a lease (``attempt_owner`` plus
``attempt_lease_expires_at``, `ATTEMPT_LEASE_SECONDS`), and every state write
is a compare-and-set on that owner and the attempt count. Retries reuse the
sealed proposal and reconcile the same output rather than create another
issue (ADR 0277), and `MarkedComments.upsert_marked` edits the one comment
`result_marker` names. A process that dies mid-attempt leaves the lease held;
the next attempt claims once it lapses. Elapsed time never proves that an
unconfirmed create was not written, so the linked issue call follows one rule:

1. An output is recorded on the delivery: the call passes it as ``expected``
   and never creates; an issue whose marker is gone is ``output_missing``.
2. No output is recorded and ``create_attempted_at`` is set: an earlier
   create may have been written, so the call passes ``reconcile_only`` and
   never creates. One match is recorded as the output and the attempt goes on
   to the comment; none is ``output_unconfirmed``; a transient read failure
   is ``tracker_unavailable`` and keeps the mark. Such a row stays owed until
   the deadline and then fails ``delivery_deadline_exceeded``, never
   delivered.
3. No output and no mark: ``create_attempted_at`` is committed under the lease
   (with the fence recheck) before the create is sent. Success records the
   output. `LinkedIssueUnconfirmed` (the request may have been written) keeps
   the mark and records ``output_unconfirmed``. A definite refusal or a failure
   before the request (``tracker_unauthorized``, ``tracker_not_found``, plain
   ``tracker_unavailable``) clears the mark in the same write, since nothing
   was created. A process that dies after the mark leaves it set.

Listing lag. A tracker's creator-filtered issue listing can lag a create by
seconds, so every ``output_unconfirmed`` sets ``next_attempt_at`` at least
`LISTING_SETTLE_SECONDS` out (the larger of that and the normal backoff), and
neither the attempt claim nor `due_deliveries` takes the row before then; a
process death is held off the same way by the attempt lease
(`ATTEMPT_LEASE_SECONDS`, 120). The wait only gives a written issue time to
appear for reconciliation. An empty listing after it proves nothing, so the
mark still forbids a create.

Rechecks. Immediately before each tracker write (the linked issue call and the
comment upsert) an attempt rechecks the deadline and fences 1 to 5 against the
locked delivery row; a failure settles failed with that code and the write is
not made, so a fence that fails between the issue and the comment posts no
comment. The transaction that records ``delivered`` repeats the same recheck
with ``clock_timestamp()`` on the locked row, so a deadline or fence crossed
after the comment still ends failed, never delivered. `expire_overdue`
settles every owed row past its deadline whether or not an attempt holds its
lease, and an attempt that then finds its row settled records nothing.

Storage boundary. Workflow rows and log lines hold identities, digests, kinds,
reason and outcome codes, timestamps and references only, never a title, body,
``tried``, ``missing``, ``observed`` or command. Submitted content lives only in
``factory_tracker_result_artifacts`` and is deleted in the same transaction
that settles the delivery (delivered or failed), or by `expire_overdue` at the
deadline. A database error writing or reading the artifact is raised as
`TrackerResultStorageError` with a code only (``artifact_write_failed``,
``artifact_read_failed``, ``delivery_write_failed``), never chained to the
SQLAlchemy error, whose text carries bound parameters.
"""

from __future__ import annotations

import enum
import hashlib
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, delete, func, or_, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.config import Settings
from curie_api.factory_comment_text import redact_factory_comment
from curie_api.factory_tracker_results import (
    FindingsProposal,
    ProposalInvalid,
    ResultProposal,
    TicketProposal,
    parse_proposal,
    proposal_content,
    proposal_sha256,
)
from curie_api.forges.capabilities import Operation, supports
from curie_api.forges.errors import (
    Ambiguous,
    LinkedIssueUnconfirmed,
    NotFound,
    Unauthorized,
    Unavailable,
    Unsupported,
)
from curie_api.forges.ports import Tracker
from curie_api.forges.types import LinkedIssue, ReplyTarget, TrackerIssueRef
from curie_api.models import (
    ExecutionRequest,
    FactoryTrackerResultArtifact,
    FactoryTrackerResultDelivery,
    FactoryTrackerResultGrant,
    FactoryTrackerResultReceipt,
    WorkItem,
)
from curie_api.tracker_result_declaration import ResultKind, canonical_json

logger = logging.getLogger("curie_api.factory_tracker_results")

RESULT_MARKER_PREFIX = "curie-tracker-result:"
ATTEMPT_LEASE_SECONDS = 120
# How long a tracker's own-identity issue listing may lag a create (seen against
# GitHub: seconds). A wait before reconciling, never proof of absence; see
# "Listing lag" above.
LISTING_SETTLE_SECONDS = 60


def output_identity_for(execution_request_id: uuid.UUID) -> str:
    """The one output identity a source execution request's grant carries."""

    return f"execution-request:{execution_request_id}"


def result_marker(execution_request_id: uuid.UUID) -> str:
    """The marker of the source issue's result comment for this request."""

    return f"{RESULT_MARKER_PREFIX}{execution_request_id}"


def source_fingerprint(work_item: WorkItem, request: ExecutionRequest) -> str:
    """SHA-256 over the durable admission facts of the source request.

    A relabel to another trigger, a base change or a re-route changes it.
    Issue content is not included.
    """

    facts = {
        "v": 1,
        "work_item": str(work_item.id),
        "request": str(request.id),
        "sequence": request.sequence,
        "tracker": [
            work_item.tracker_kind,
            work_item.tracker_host,
            work_item.tracker_scope_id,
            work_item.tracker_issue_id,
        ],
        "repository": [
            work_item.code_host_kind,
            work_item.code_host_host,
            work_item.repository_project_id,
        ],
        "installation": work_item.code_host_installation_id,
        "agent": str(work_item.agent_id),
        "trigger_label": work_item.trigger_label,
        "base": [work_item.base_branch, work_item.base_commit],
    }
    return hashlib.sha256(canonical_json(facts)).hexdigest()


class DeliveryState(enum.StrEnum):
    OWED = "owed"
    DELIVERED = "delivered"
    FAILED = "failed"


class RefusalReason(enum.StrEnum):
    GRANT_NOT_FOUND = "grant_not_found"
    GRANT_REQUEST_MISMATCH = "grant_request_mismatch"
    GRANT_SCOPE_MISMATCH = "grant_scope_mismatch"
    GRANT_EXPIRED = "grant_expired"
    RESULT_KIND_NOT_ALLOWED = "result_kind_not_allowed"
    SOURCE_FINGERPRINT_CHANGED = "source_fingerprint_changed"
    OWNER_NOT_LIVE = "owner_not_live"
    CANCELLED = "cancelled"
    TRACKER_MISMATCH = "tracker_mismatch"
    OPERATION_UNSUPPORTED = "tracker_operation_unsupported"
    CLOSES_SOURCE = "closes_source"
    PROPOSAL_CONFLICT = "proposal_conflict"


class AttemptError(enum.StrEnum):
    """``last_error_code`` of an attempt that leaves the delivery owed."""

    TRACKER_UNAVAILABLE = "tracker_unavailable"
    TRACKER_UNAUTHORIZED = "tracker_unauthorized"
    TRACKER_NOT_FOUND = "tracker_not_found"
    READBACK_MISMATCH = "readback_mismatch"
    OUTPUT_MISMATCH = "output_mismatch"
    OUTPUT_MISSING = "output_missing"
    OUTPUT_UNCONFIRMED = "output_unconfirmed"


class FailureCode(enum.StrEnum):
    """``failure_code`` on state failed, besides every `RefusalReason` value."""

    DEADLINE_EXCEEDED = "delivery_deadline_exceeded"
    OUTPUT_AMBIGUOUS = "output_ambiguous"


class TrackerResultRefused(Exception):
    """A fence refused the submission; nothing was written."""

    def __init__(self, reason: RefusalReason) -> None:
        self.reason = reason
        super().__init__(f"tracker result refused: {reason.value}")


class TrackerResultStorageError(Exception):
    """A database error on the delivery or its transport artifact, as a code only.

    Raised in place of the SQLAlchemy error, never chained to it: that error's
    text carries bound parameters, which for the artifact is submitted content.
    """

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"tracker result storage failed: {code}")


@dataclass(frozen=True)
class DeliveryOutcome:
    delivery_id: uuid.UUID
    state: DeliveryState
    # False: settled already, or another attempt holds the lease.
    attempted: bool
    last_error_code: str | None
    failure_code: str | None


# Fences ---------------------------------------------------------------------


@dataclass(frozen=True)
class _Facts:
    grant: FactoryTrackerResultGrant
    work_item: WorkItem
    request: ExecutionRequest
    now: datetime


async def _read_facts(session: AsyncSession, grant_id: uuid.UUID) -> _Facts | None:
    row = (
        await session.execute(
            select(FactoryTrackerResultGrant, WorkItem, ExecutionRequest, func.clock_timestamp())
            .select_from(FactoryTrackerResultGrant)
            .join(
                ExecutionRequest,
                ExecutionRequest.id == FactoryTrackerResultGrant.execution_request_id,
            )
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .where(FactoryTrackerResultGrant.id == grant_id)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    if row is None:
        return None
    grant, work_item, request, now = row
    return _Facts(grant=grant, work_item=work_item, request=request, now=now)


_CLOSING_KEYWORD = r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b:?\s*"


def _closes_source(texts: tuple[str, ...], references: tuple[str, ...]) -> bool:
    alternatives = "|".join(re.escape(reference) for reference in references if reference)
    pattern = re.compile(_CLOSING_KEYWORD + f"(?:{alternatives})(?![0-9A-Za-z])", re.IGNORECASE)
    return any(pattern.search(text) for text in texts)


def _source_references(tracker: Tracker, work_item: WorkItem) -> tuple[str, ...] | None:
    source = work_item.tracker_issue
    try:
        url = tracker.issue_url(source)
    except NotFound:
        return None
    references = [
        f"#{source.issue_id}",
        f"{work_item.repository_path}#{source.issue_id}",
        url,
    ]
    if source.display_key:
        references.append(source.display_key)
    return tuple(references)


def _refusal(
    facts: _Facts | None,
    *,
    execution_request_id: uuid.UUID,
    runtime_epoch: int,
    tracker: Tracker,
    result_kind: str,
    proposal: ResultProposal | None,
) -> RefusalReason | None:
    """The first fence that fails, or None. ``proposal`` is given at submission only."""

    if facts is None:
        return RefusalReason.GRANT_NOT_FOUND
    grant, work_item, request, now = facts.grant, facts.work_item, facts.request, facts.now
    if grant.execution_request_id != execution_request_id:
        return RefusalReason.GRANT_REQUEST_MISMATCH
    if (
        grant.work_item_id != request.work_item_id
        or grant.repository_id != work_item.repository_project_id
    ):
        return RefusalReason.GRANT_SCOPE_MISMATCH
    if proposal is not None:
        if grant.expires_at <= now:
            return RefusalReason.GRANT_EXPIRED
        if result_kind not in grant.allowed_result_kinds:
            return RefusalReason.RESULT_KIND_NOT_ALLOWED
    if grant.source_fingerprint != source_fingerprint(work_item, request):
        return RefusalReason.SOURCE_FINGERPRINT_CHANGED
    if work_item.cancelled_at is not None or request.cancellation_requested_at is not None:
        return RefusalReason.CANCELLED
    if (
        request.status != "running"
        or request.runtime_epoch != runtime_epoch
        or not request.runtime_owner
        or request.runtime_heartbeat_expires_at is None
        or request.runtime_heartbeat_expires_at <= now
        or request.execution_deadline is None
        or request.execution_deadline <= now
    ):
        return RefusalReason.OWNER_NOT_LIVE
    if tracker.kind != work_item.tracker_kind or tracker.host != work_item.tracker_host:
        return RefusalReason.TRACKER_MISMATCH
    if result_kind == ResultKind.TICKET and not supports(
        tracker.capabilities, Operation.CREATE_LINKED_ISSUE
    ):
        return RefusalReason.OPERATION_UNSUPPORTED
    if isinstance(proposal, TicketProposal):
        references = _source_references(tracker, work_item)
        if references is None:
            return RefusalReason.TRACKER_MISMATCH
        if _closes_source((proposal.title, proposal.body), references):
            return RefusalReason.CLOSES_SOURCE
    return None


# Submission -----------------------------------------------------------------


async def submit_result(
    session: AsyncSession,
    *,
    grant_id: uuid.UUID,
    execution_request_id: uuid.UUID,
    runtime_epoch: int,
    proposal: ResultProposal,
    tracker: Tracker,
    settings: Settings,
) -> uuid.UUID:
    """Seal ``proposal`` under the grant as one owed delivery and return its id.

    One committed transaction inserts the delivery row and its transport
    artifact. Nothing is written to the tracker. Raises `TrackerResultRefused`.
    """

    digest = proposal_sha256(proposal)
    facts = await _read_facts(session, grant_id)
    refusal = _refusal(
        facts,
        execution_request_id=execution_request_id,
        runtime_epoch=runtime_epoch,
        tracker=tracker,
        result_kind=proposal.kind,
        proposal=proposal,
    )
    if refusal is not None:
        await session.rollback()
        logger.info(
            "tracker result refused grant=%s request=%s kind=%s reason=%s",
            grant_id,
            execution_request_id,
            proposal.kind,
            refusal.value,
        )
        raise TrackerResultRefused(refusal)
    assert facts is not None
    existing = await _existing(session, grant_id, digest)
    if existing is not None:
        await session.rollback()
        return existing
    now = facts.now
    deadline = now + timedelta(seconds=settings.factory_tracker_result_delivery_deadline_seconds)
    delivery_id = uuid.uuid4()
    session.add(
        FactoryTrackerResultDelivery(
            id=delivery_id,
            grant_id=grant_id,
            execution_request_id=execution_request_id,
            result_kind=proposal.kind,
            reason=proposal.reason.value if isinstance(proposal, FindingsProposal) else None,
            proposal_sha256=digest,
            runtime_epoch=runtime_epoch,
            state=DeliveryState.OWED.value,
            attempts=0,
            next_attempt_at=now,
            submitted_at=now,
            deadline_at=deadline,
        )
    )
    duplicate = False
    try:
        await session.flush()
    except IntegrityError:
        duplicate = True
    if duplicate:
        # A concurrent submission for the same grant won the unique key.
        await session.rollback()
        existing = await _existing(session, grant_id, digest)
        await session.rollback()
        if existing is None:
            raise TrackerResultStorageError("delivery_write_failed")
        return existing
    session.add(
        FactoryTrackerResultArtifact(
            delivery_id=delivery_id, content=proposal_content(proposal), expires_at=deadline
        )
    )
    # Raised outside the handler so the code-only error never chains the
    # SQLAlchemy error, whose text holds the artifact's bound content.
    write_failed = False
    try:
        await session.flush()
        await session.commit()
    except SQLAlchemyError:
        write_failed = True
    if write_failed:
        await session.rollback()
        logger.warning(
            "tracker result artifact write failed grant=%s request=%s kind=%s",
            grant_id,
            execution_request_id,
            proposal.kind,
        )
        raise TrackerResultStorageError("artifact_write_failed")
    logger.info(
        "tracker result sealed delivery=%s grant=%s request=%s kind=%s",
        delivery_id,
        grant_id,
        execution_request_id,
        proposal.kind,
    )
    return delivery_id


async def _existing(session: AsyncSession, grant_id: uuid.UUID, digest: str) -> uuid.UUID | None:
    """The grant's delivery when it sealed ``digest``; refuses another digest."""

    row = (
        await session.execute(
            select(
                FactoryTrackerResultDelivery.id, FactoryTrackerResultDelivery.proposal_sha256
            ).where(FactoryTrackerResultDelivery.grant_id == grant_id)
        )
    ).one_or_none()
    if row is None:
        return None
    if row.proposal_sha256 != digest:
        await session.rollback()
        logger.info(
            "tracker result refused grant=%s delivery=%s reason=%s",
            grant_id,
            row.id,
            RefusalReason.PROPOSAL_CONFLICT.value,
        )
        raise TrackerResultRefused(RefusalReason.PROPOSAL_CONFLICT)
    return uuid.UUID(str(row.id))


# Rendering ------------------------------------------------------------------


def _publishable(text: str) -> str:
    """Redacted, with no HTML comment opener left to carry a marker, and no
    live mention.

    The tracker posts this text under the App's identity, so a model-written
    ``@login`` would notify that account. A U+200B after every ``@`` keeps the
    text readable and stops the mention, as `factory_notices` does for the
    agent's last message.
    """

    return redact_factory_comment(text).replace("<!--", "&lt;!--").replace("@", "@\u200b")


def _fenced(text: str) -> str:
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    return f"{fence}\n{text}\n{fence}"


def _ticket_comment(proposal: TicketProposal, url: str) -> str:
    reproduction = proposal.reproduction
    return "\n".join(
        [
            f"Published an implementation ticket from this report: {url}",
            "",
            f"Investigated revision: `{reproduction.revision}`",
            "",
            "Reproduction command:",
            "",
            _fenced(_publishable(reproduction.command)),
            "",
            "Observed result:",
            "",
            _fenced(_publishable(reproduction.observed)),
        ]
    )


def _findings_comment(proposal: FindingsProposal) -> str:
    lines = [
        "No implementation ticket was produced from this report.",
        "",
        f"Reason: `{proposal.reason.value}`",
    ]
    if proposal.revision is not None:
        lines.append(f"Investigated revision: `{proposal.revision}`")
    lines += [
        "",
        "What was tried:",
        "",
        _publishable(proposal.tried),
        "",
        "What is missing:",
        "",
        _publishable(proposal.missing),
    ]
    return "\n".join(lines)


# Attempts -------------------------------------------------------------------

# A verdict meaning this attempt no longer holds the row (lease taken over, or
# the row already settled). Never stored.
_LEASE_LOST = "lease_lost"


@dataclass(frozen=True)
class _Claim:
    delivery_id: uuid.UUID
    grant_id: uuid.UUID
    execution_request_id: uuid.UUID
    result_kind: str
    proposal_sha256: str
    runtime_epoch: int
    attempts: int
    owner: str
    deadline_passed: bool
    # The output issue an earlier attempt recorded, or None.
    recorded: TrackerIssueRef | None
    # An earlier attempt sent (or may have sent) a create whose outcome is uncertain.
    create_attempted: bool


def _held(claim: _Claim) -> tuple[ColumnElement[bool], ...]:
    """The compare-and-set every write of this attempt carries."""

    return (
        FactoryTrackerResultDelivery.id == claim.delivery_id,
        FactoryTrackerResultDelivery.state == DeliveryState.OWED.value,
        FactoryTrackerResultDelivery.attempt_owner == claim.owner,
        FactoryTrackerResultDelivery.attempts == claim.attempts,
    )


async def _current(
    session: AsyncSession, delivery_id: uuid.UUID, *, attempted: bool
) -> DeliveryOutcome:
    row = (
        await session.execute(
            select(
                FactoryTrackerResultDelivery.state,
                FactoryTrackerResultDelivery.last_error_code,
                FactoryTrackerResultDelivery.failure_code,
            ).where(FactoryTrackerResultDelivery.id == delivery_id)
        )
    ).one_or_none()
    if row is None:
        raise LookupError("no tracker result delivery with that id")
    return DeliveryOutcome(
        delivery_id=delivery_id,
        state=DeliveryState(row.state),
        attempted=attempted,
        last_error_code=row.last_error_code,
        failure_code=row.failure_code,
    )


async def _claim(
    sessionmaker: async_sessionmaker[AsyncSession], delivery_id: uuid.UUID, owner: str
) -> _Claim | DeliveryOutcome:
    delivery = FactoryTrackerResultDelivery
    async with sessionmaker() as session:
        row = (
            await session.execute(
                update(delivery)
                .where(
                    delivery.id == delivery_id,
                    delivery.state == DeliveryState.OWED.value,
                    or_(
                        delivery.attempt_owner.is_(None),
                        delivery.attempt_lease_expires_at <= func.clock_timestamp(),
                    ),
                    # Retry eligibility, enforced here for every caller. Past the
                    # deadline the attempt only settles failed, so it may run.
                    or_(
                        delivery.next_attempt_at <= func.clock_timestamp(),
                        delivery.deadline_at <= func.clock_timestamp(),
                    ),
                )
                .values(
                    attempt_owner=owner,
                    attempt_lease_expires_at=func.clock_timestamp()
                    + timedelta(seconds=ATTEMPT_LEASE_SECONDS),
                    attempts=delivery.attempts + 1,
                )
                .returning(
                    delivery.grant_id,
                    delivery.execution_request_id,
                    delivery.result_kind,
                    delivery.proposal_sha256,
                    delivery.runtime_epoch,
                    delivery.attempts,
                    (delivery.deadline_at <= func.clock_timestamp()).label("deadline_passed"),
                    delivery.output_tracker_kind,
                    delivery.output_tracker_host,
                    delivery.output_scope_id,
                    delivery.output_issue_id,
                    delivery.create_attempted_at.is_not(None).label("create_attempted"),
                )
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
        if row is None:
            outcome = await _current(session, delivery_id, attempted=False)
            await session.rollback()
            return outcome
        await session.commit()
    recorded = (
        TrackerIssueRef(
            row.output_tracker_kind,
            row.output_tracker_host,
            row.output_scope_id,
            row.output_issue_id,
        )
        if row.output_issue_id is not None
        else None
    )
    return _Claim(
        delivery_id=delivery_id,
        grant_id=row.grant_id,
        execution_request_id=row.execution_request_id,
        result_kind=row.result_kind,
        proposal_sha256=row.proposal_sha256,
        runtime_epoch=int(row.runtime_epoch),
        attempts=int(row.attempts),
        owner=owner,
        deadline_passed=bool(row.deadline_passed),
        recorded=recorded,
        create_attempted=bool(row.create_attempted),
    )


async def _verdict(session: AsyncSession, claim: _Claim, tracker: Tracker) -> str | None:
    """None when this attempt may write now; else a failure code or `_LEASE_LOST`.

    Locks the delivery row, so in the caller's transaction neither the
    deadline sweep nor another attempt can settle it underneath.
    """

    passed = await session.scalar(
        select(FactoryTrackerResultDelivery.deadline_at <= func.clock_timestamp())
        .where(*_held(claim))
        .with_for_update()
    )
    if passed is None:
        return _LEASE_LOST
    if passed:
        return FailureCode.DEADLINE_EXCEEDED.value
    refusal = _refusal(
        await _read_facts(session, claim.grant_id),
        execution_request_id=claim.execution_request_id,
        runtime_epoch=claim.runtime_epoch,
        tracker=tracker,
        result_kind=claim.result_kind,
        proposal=None,
    )
    return None if refusal is None else refusal.value


async def _fail_in(session: AsyncSession, claim: _Claim, code: str) -> bool:
    """Settle failed under the lease and delete the artifact; the caller commits."""

    settled = await session.scalar(
        update(FactoryTrackerResultDelivery)
        .where(*_held(claim))
        .values(
            state=DeliveryState.FAILED.value,
            failure_code=code,
            settled_at=func.clock_timestamp(),
            attempt_owner=None,
            attempt_lease_expires_at=None,
        )
        .returning(FactoryTrackerResultDelivery.id)
        .execution_options(synchronize_session=False)
    )
    if settled is None:
        return False
    await session.execute(
        delete(FactoryTrackerResultArtifact).where(
            FactoryTrackerResultArtifact.delivery_id == claim.delivery_id
        )
    )
    return True


def _log_failed(claim: _Claim, code: str, settled: bool) -> None:
    logger.info(
        "tracker result failed delivery=%s request=%s kind=%s attempts=%d code=%s settled=%s",
        claim.delivery_id,
        claim.execution_request_id,
        claim.result_kind,
        claim.attempts,
        code,
        settled,
    )


async def _close(session: AsyncSession, claim: _Claim, verdict: str) -> DeliveryOutcome:
    """End the attempt on a verdict: settle failed with its code, or record nothing."""

    if verdict == _LEASE_LOST:
        await session.rollback()
        logger.info(
            "tracker result attempt lost its lease delivery=%s attempts=%d",
            claim.delivery_id,
            claim.attempts,
        )
    else:
        settled = await _fail_in(session, claim, verdict)
        await session.commit()
        _log_failed(claim, verdict, settled)
    outcome = await _current(session, claim.delivery_id, attempted=True)
    await session.rollback()
    return outcome


async def _settle_failed(
    sessionmaker: async_sessionmaker[AsyncSession], claim: _Claim, code: str
) -> DeliveryOutcome:
    async with sessionmaker() as session:
        return await _close(session, claim, code)


async def _gate(
    sessionmaker: async_sessionmaker[AsyncSession],
    claim: _Claim,
    tracker: Tracker,
    *,
    mark_create: bool = False,
) -> DeliveryOutcome | None:
    """Recheck the deadline and every fence immediately before a tracker write.

    None lets the write go ahead. Otherwise the attempt has ended: failed with
    the code, or nothing recorded when the row is no longer this attempt's.
    ``mark_create`` also sets ``create_attempted_at`` in the same committed
    transaction, before any create request is sent.
    """

    async with sessionmaker() as session:
        verdict = await _verdict(session, claim, tracker)
        if verdict is None:
            if mark_create:
                await session.execute(
                    update(FactoryTrackerResultDelivery)
                    .where(*_held(claim))
                    .values(create_attempted_at=func.clock_timestamp())
                    .execution_options(synchronize_session=False)
                )
                await session.commit()
            else:
                await session.rollback()
            return None
        return await _close(session, claim, verdict)


async def _record_error(
    sessionmaker: async_sessionmaker[AsyncSession],
    claim: _Claim,
    code: AttemptError,
    settings: Settings,
    *,
    at_least_seconds: int = 0,
    clear_create_attempt: bool = False,
) -> DeliveryOutcome:
    """Record ``code``, back off, release the lease; the delivery stays owed.

    ``next_attempt_at`` moves out by the WorkItem backoff, or by
    ``at_least_seconds`` when that is longer. ``clear_create_attempt`` clears
    ``create_attempted_at`` in the same write: the create was definitely not
    written.
    """

    backoff = max(
        min(
            settings.work_item_backoff_base_seconds * 2 ** max(claim.attempts - 1, 0),
            settings.work_item_backoff_max_seconds,
        ),
        at_least_seconds,
    )
    async with sessionmaker() as session:
        await session.execute(
            update(FactoryTrackerResultDelivery)
            .where(*_held(claim))
            .values(
                last_error_code=code.value,
                next_attempt_at=func.clock_timestamp() + timedelta(seconds=backoff),
                attempt_owner=None,
                attempt_lease_expires_at=None,
                **({"create_attempted_at": None} if clear_create_attempt else {}),
            )
            .execution_options(synchronize_session=False)
        )
        await session.commit()
        outcome = await _current(session, claim.delivery_id, attempted=True)
        await session.rollback()
    logger.info(
        "tracker result attempt failed delivery=%s request=%s kind=%s attempts=%d code=%s",
        claim.delivery_id,
        claim.execution_request_id,
        claim.result_kind,
        claim.attempts,
        code.value,
    )
    return outcome


async def _record_output(
    sessionmaker: async_sessionmaker[AsyncSession], claim: _Claim, linked: LinkedIssue
) -> AttemptError | None:
    """Record the output issue on the delivery; ``output_mismatch`` when another is recorded.

    None also when the lease was lost: the next write's compare-and-set then
    records nothing.
    """

    delivery = FactoryTrackerResultDelivery
    async with sessionmaker() as session:
        row = (
            await session.execute(
                select(
                    delivery.output_tracker_kind,
                    delivery.output_tracker_host,
                    delivery.output_scope_id,
                    delivery.output_issue_id,
                )
                .where(*_held(claim))
                .with_for_update()
            )
        ).one_or_none()
        if row is None:
            await session.rollback()
            return None
        issue = linked.issue
        wanted = (issue.kind, issue.host, issue.scope_id, issue.issue_id)
        if row.output_issue_id is not None:
            await session.rollback()
            return None if tuple(row) == wanted else AttemptError.OUTPUT_MISMATCH
        await session.execute(
            update(delivery)
            .where(*_held(claim))
            .values(
                output_tracker_kind=issue.kind,
                output_tracker_host=issue.host,
                output_scope_id=issue.scope_id,
                output_issue_id=issue.issue_id,
                output_url=linked.url,
                output_body_sha256=linked.body_sha256,
            )
            .execution_options(synchronize_session=False)
        )
        await session.commit()
    return None


async def _settle_delivered(
    sessionmaker: async_sessionmaker[AsyncSession],
    claim: _Claim,
    proposal: ResultProposal,
    *,
    tracker: Tracker,
    comment_id: str,
    linked: LinkedIssue | None,
) -> DeliveryOutcome:
    """Receipt, ``delivered`` and artifact deletion in one transaction under the lease.

    The same transaction first rechecks the deadline and every fence on the
    locked row (`_verdict`); a failure settles failed instead, never delivered.
    """

    async with sessionmaker() as session:
        verdict = await _verdict(session, claim, tracker)
        if verdict is not None:
            return await _close(session, claim, verdict)
        await session.execute(
            update(FactoryTrackerResultDelivery)
            .where(*_held(claim))
            .values(
                state=DeliveryState.DELIVERED.value,
                settled_at=func.clock_timestamp(),
                last_error_code=None,
                attempt_owner=None,
                attempt_lease_expires_at=None,
            )
            .execution_options(synchronize_session=False)
        )
        if isinstance(proposal, TicketProposal):
            revision: str | None = proposal.reproduction.revision
            reason = None
        else:
            revision = proposal.revision
            reason = proposal.reason.value
        output = linked.issue if linked is not None else None
        session.add(
            FactoryTrackerResultReceipt(
                delivery_id=claim.delivery_id,
                grant_id=claim.grant_id,
                execution_request_id=claim.execution_request_id,
                result_kind=claim.result_kind,
                reason=reason,
                proposal_sha256=claim.proposal_sha256,
                revision=revision,
                result_comment_id=comment_id,
                output_tracker_kind=output.kind if output else None,
                output_tracker_host=output.host if output else None,
                output_scope_id=output.scope_id if output else None,
                output_issue_id=output.issue_id if output else None,
                output_url=linked.url if linked else None,
                output_body_sha256=linked.body_sha256 if linked else None,
                output_labels=sorted(linked.labels) if linked else None,
            )
        )
        await session.execute(
            delete(FactoryTrackerResultArtifact).where(
                FactoryTrackerResultArtifact.delivery_id == claim.delivery_id
            )
        )
        await session.commit()
    logger.info(
        "tracker result delivered delivery=%s request=%s kind=%s attempts=%d",
        claim.delivery_id,
        claim.execution_request_id,
        claim.result_kind,
        claim.attempts,
    )
    return DeliveryOutcome(
        delivery_id=claim.delivery_id,
        state=DeliveryState.DELIVERED,
        attempted=True,
        last_error_code=None,
        failure_code=None,
    )


@dataclass(frozen=True)
class _Sealed:
    proposal: ResultProposal
    source: TrackerIssueRef
    output_identity: str


async def _load_sealed(
    sessionmaker: async_sessionmaker[AsyncSession], claim: _Claim
) -> _Sealed | str:
    """The sealed proposal and its destination, or the failure code that ends the delivery.

    A database error reading the artifact is re-raised as a code-only
    `TrackerResultStorageError`, never chained: SQLAlchemy error text carries
    bound parameters and row data.
    """

    read_failed = False
    async with sessionmaker() as session:
        facts = await _read_facts(session, claim.grant_id)
        if facts is None:
            await session.rollback()
            return RefusalReason.GRANT_NOT_FOUND.value
        source = facts.work_item.tracker_issue
        output_identity = facts.grant.output_identity
        content: object = None
        try:
            content = await session.scalar(
                select(FactoryTrackerResultArtifact.content).where(
                    FactoryTrackerResultArtifact.delivery_id == claim.delivery_id
                )
            )
        except SQLAlchemyError:
            read_failed = True
        await session.rollback()
    if read_failed:
        logger.warning("tracker result artifact read failed delivery=%s", claim.delivery_id)
        raise TrackerResultStorageError("artifact_read_failed")
    if content is None:
        return RefusalReason.PROPOSAL_CONFLICT.value
    try:
        proposal = parse_proposal(content)
    except ProposalInvalid:
        return RefusalReason.PROPOSAL_CONFLICT.value
    if proposal_sha256(proposal) != claim.proposal_sha256 or proposal.kind != claim.result_kind:
        return RefusalReason.PROPOSAL_CONFLICT.value
    return _Sealed(proposal=proposal, source=source, output_identity=output_identity)


async def _linked_issue(
    sessionmaker: async_sessionmaker[AsyncSession],
    claim: _Claim,
    tracker: Tracker,
    sealed: _Sealed,
    proposal: TicketProposal,
    settings: Settings,
) -> LinkedIssue | DeliveryOutcome:
    """The output issue, by the one rule that never creates twice (see "Exactly once").

    1. An output is recorded: ``expected`` reads it; gone is ``output_missing``.
    2. A create was attempted and its outcome is uncertain: ``reconcile_only``
       never creates; none found is ``output_unconfirmed``.
    3. Otherwise ``create_attempted_at`` is committed first, then the create is
       sent. An unconfirmed create keeps the mark (``output_unconfirmed``); a
       definite refusal clears it in the same write that records the error.

    Ambiguous and Unsupported propagate to `deliver`, which settles failed.
    """

    reconcile_only = claim.recorded is None and claim.create_attempted
    first_create = claim.recorded is None and not claim.create_attempted
    stopped = await _gate(sessionmaker, claim, tracker, mark_create=first_create)
    if stopped is not None:
        return stopped
    error: AttemptError
    settle = 0
    clear = False
    try:
        return await tracker.create_linked_issue(
            sealed.source,
            output_identity=sealed.output_identity,
            title=_publishable(proposal.title),
            body=_publishable(proposal.body),
            # The implementation label is the handoff's to apply (#5588).
            labels=(),
            expected=claim.recorded,
            reconcile_only=reconcile_only,
        )
    except LinkedIssueUnconfirmed:
        # The create may be written; it is never sent again.
        error, settle = AttemptError.OUTPUT_UNCONFIRMED, LISTING_SETTLE_SECONDS
    except NotFound:
        if claim.recorded is not None:
            error = AttemptError.OUTPUT_MISSING
        elif reconcile_only:
            error, settle = AttemptError.OUTPUT_UNCONFIRMED, LISTING_SETTLE_SECONDS
        else:
            error, clear = AttemptError.TRACKER_NOT_FOUND, True
    except Unavailable:
        error, clear = AttemptError.TRACKER_UNAVAILABLE, first_create
    except Unauthorized:
        error, clear = AttemptError.TRACKER_UNAUTHORIZED, first_create
    return await _record_error(
        sessionmaker,
        claim,
        error,
        settings,
        at_least_seconds=settle,
        clear_create_attempt=clear,
    )


async def deliver(
    sessionmaker: async_sessionmaker[AsyncSession],
    delivery_id: uuid.UUID,
    *,
    tracker: Tracker,
    settings: Settings,
    owner: str,
) -> DeliveryOutcome:
    """One delivery attempt: claim, then before each tracker write recheck the
    deadline and every fence, write, read back, and settle under a final recheck.

    Catches only the forge errors of the ports. Anything else, a
    `BaseException` included, escapes with the lease held, which is how a
    process restart looks; the next attempt claims once the lease lapses.
    A row another attempt holds, or whose ``next_attempt_at`` is still ahead
    and whose deadline has not passed, is not claimed: ``attempted`` False.
    """

    claim = await _claim(sessionmaker, delivery_id, owner)
    if isinstance(claim, DeliveryOutcome):
        return claim
    if claim.deadline_passed:
        return await _settle_failed(sessionmaker, claim, FailureCode.DEADLINE_EXCEEDED.value)
    sealed = await _load_sealed(sessionmaker, claim)
    if isinstance(sealed, str):
        return await _settle_failed(sessionmaker, claim, sealed)
    proposal = sealed.proposal
    target = ReplyTarget.on_issue(sealed.source)
    marker = result_marker(claim.execution_request_id)
    linked: LinkedIssue | None = None
    try:
        if isinstance(proposal, TicketProposal):
            got = await _linked_issue(sessionmaker, claim, tracker, sealed, proposal, settings)
            if isinstance(got, DeliveryOutcome):
                return got
            linked = got
            if linked.body_sha256 != linked.sent_body_sha256:
                return await _record_error(
                    sessionmaker, claim, AttemptError.READBACK_MISMATCH, settings
                )
            mismatch = await _record_output(sessionmaker, claim, linked)
            if mismatch is not None:
                return await _record_error(sessionmaker, claim, mismatch, settings)
            rendered = _ticket_comment(proposal, linked.url)
        else:
            rendered = _findings_comment(proposal)
        stopped = await _gate(sessionmaker, claim, tracker)
        if stopped is not None:
            return stopped
        upserted = await tracker.marked_comments.upsert_marked(target, marker, rendered)
        found = await tracker.marked_comments.find_marked(target, marker)
    except Ambiguous:
        return await _settle_failed(sessionmaker, claim, FailureCode.OUTPUT_AMBIGUOUS.value)
    except Unsupported:
        return await _settle_failed(sessionmaker, claim, RefusalReason.OPERATION_UNSUPPORTED.value)
    except Unavailable:
        return await _record_error(sessionmaker, claim, AttemptError.TRACKER_UNAVAILABLE, settings)
    except Unauthorized:
        return await _record_error(sessionmaker, claim, AttemptError.TRACKER_UNAUTHORIZED, settings)
    except NotFound:
        return await _record_error(sessionmaker, claim, AttemptError.TRACKER_NOT_FOUND, settings)
    if (
        found is None
        or found.id != upserted.comment.id
        or (linked is not None and linked.url not in found.body)
    ):
        return await _record_error(sessionmaker, claim, AttemptError.READBACK_MISMATCH, settings)
    return await _settle_delivered(
        sessionmaker, claim, proposal, tracker=tracker, comment_id=found.id, linked=linked
    )


# Sweeps ---------------------------------------------------------------------


async def expire_overdue(sessionmaker: async_sessionmaker[AsyncSession], *, limit: int = 50) -> int:
    """Settle up to ``limit`` owed deliveries past their deadline, leased or not.

    Each becomes failed ``delivery_deadline_exceeded`` and loses its artifact
    in the same transaction. An attempt still running on such a row then
    finds it settled and records nothing. Rows an attempt has locked for its
    final recheck are skipped until the next sweep. Not scheduled by anything
    in #5585.
    """

    delivery = FactoryTrackerResultDelivery
    async with sessionmaker() as session:
        due = (
            select(delivery.id)
            .where(
                delivery.state == DeliveryState.OWED.value,
                delivery.deadline_at <= func.clock_timestamp(),
            )
            .order_by(delivery.deadline_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        expired = list(
            (
                await session.scalars(
                    update(delivery)
                    .where(delivery.id.in_(due))
                    .values(
                        state=DeliveryState.FAILED.value,
                        failure_code=FailureCode.DEADLINE_EXCEEDED.value,
                        settled_at=func.clock_timestamp(),
                        attempt_owner=None,
                        attempt_lease_expires_at=None,
                    )
                    .returning(delivery.id)
                    .execution_options(synchronize_session=False)
                )
            ).all()
        )
        if expired:
            await session.execute(
                delete(FactoryTrackerResultArtifact).where(
                    FactoryTrackerResultArtifact.delivery_id.in_(expired)
                )
            )
        await session.commit()
    for delivery_id in expired:
        logger.info(
            "tracker result failed delivery=%s code=%s",
            delivery_id,
            FailureCode.DEADLINE_EXCEEDED.value,
        )
    return len(expired)


async def due_deliveries(session: AsyncSession, *, limit: int = 50) -> list[uuid.UUID]:
    """Owed deliveries whose backoff has passed and whose lease is free or lapsed."""

    delivery = FactoryTrackerResultDelivery
    ids = await session.scalars(
        select(delivery.id)
        .where(
            delivery.state == DeliveryState.OWED.value,
            delivery.next_attempt_at <= func.clock_timestamp(),
            or_(
                delivery.attempt_owner.is_(None),
                delivery.attempt_lease_expires_at <= func.clock_timestamp(),
            ),
        )
        .order_by(delivery.next_attempt_at)
        .limit(limit)
    )
    return list(ids)
