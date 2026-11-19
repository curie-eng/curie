"""Transactional lifecycle operations for durable WorkItems.

Each public operation owns and commits the supplied session transaction,
including conflict outcomes. Callers should dedicate the session to one operation
and avoid attaching unrelated pending work.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from curie_internal.requester_coding_instructions import EvidenceState
from curie_telemetry.redact import redact_text
from sqlalchemy import and_, case, exists, func, literal, or_, select, update
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from .. import factory_questions, factory_retry_policy, transcripts
from ..config import get_settings
from ..factory_reply_target import is_issue_follow_up, is_relabel_objective
from ..factory_retry_policy import Disposition, DispositionKind, RetryFacts, RetrySafety
from ..factory_tracker_result_delivery import DeliveryOrigin, DeliveryState, seal_platform_findings
from ..factory_tracker_result_runtime import (
    CONTRACT_IMPLEMENTATION,
    CONTRACT_TRACKER_RESULT,
    CompletionContractName,
    ContractRefused,
    select_completion_contract,
)
from ..forges.errors import ForgeError
from ..forges.registry import forges_registry
from ..forges.trackers import comment_prefix, issue_link, reply_route
from ..forges.types import RepositoryRef, TrackerIssueRef
from ..models import (
    DEFAULT_EXECUTION_DEADLINE_SECONDS,
    Agent,
    Deployment,
    ExecutionRequest,
    FactoryBaseTransition,
    FactoryStatusComment,
    FactoryTrackerResultDelivery,
    FactoryTrackerResultGrant,
    FactoryTrackerResultReceipt,
    FactoryTriageDelegation,
    Publication,
    ThreadPublicationLineage,
    WorkItem,
    new_card_token,
)
from ..personal_execution import (
    ModelAuthMode,
    OwnerMismatchCode,
    OwnerTuple,
    assert_no_owner_columns,
    compare_owner,
    mode_of,
    snapshot_columns,
)
from ..requester_instruction_admission import (
    add_snapshot,
    evidence,
    metadata,
    request_selection_columns,
    selection_columns,
)
from ..schemas.workitems import CodingInstructionsMetadata
from . import resource_gate

if TYPE_CHECKING:
    # factory_base reaches this module through factory_notices at import time.
    from ..factory_base import ResolvedBase

RequestStatus = Literal[
    "queued",
    "waiting",
    "running",
    "cancellation_requested",
    "completed",
    "failed",
    "expired",
    "cancelled",
]

ConflictCode = Literal[
    "not_found",
    "identity_mismatch",
    "stale_version",
    "active_request",
    "work_item_cancelled",
    "illegal_transition",
    "waiting_deadline_elapsed",
    "execution_deadline_elapsed",
    "lineage_mismatch",
    "lineage_already_owned",
    "publication_ineligible",
    "publication_pending",
    "termination_observation_required",
    # A gated request's start did not deliver a strictly larger limit (#5427).
    "resource_gate_unmet",
    # An explicit personal owner that differs from the WorkItem it raced to (#5376).
    OwnerMismatchCode,
]

_ACTIVE_STATUSES = ("waiting", "running", "cancellation_requested")
IN_FLIGHT_PUBLICATION = ("pending", "approved", "launching", "running")
AWAITING_PUBLICATION = (*IN_FLIGHT_PUBLICATION, "succeeded")
# A failed finish that never published defers to an in-flight publication (#2577, #3128).
_UNPUBLISHED_CAUSES = frozenset({"no_pull_request", "early_stop", "revision_unchanged"})
# Longest provider message a factory notice keeps (#3073).
_NOTICE_DETAIL_MAX = 4000
# Automatic fresh requests a WorkItem may receive over its lifetime, shared by
# owner loss and ordinary failures (ADR 0269).
AUTOMATIC_RETRY_LIMIT = 2
# Every condition a blocked successor may wait on has a releaser: a provider
# probe or a resource gate (#5424).
_RELEASED_CONDITIONS: Final = factory_retry_policy.PROVIDER_GATES | resource_gate.GATE_CONDITIONS
# Backoff before each slot's successor, and the deterministic jitter around it.
AUTOMATIC_RETRY_DELAYS_S = (30, 120)
AUTOMATIC_RETRY_JITTER = 0.2
# Started terminal statuses whose runtime teardown an enrollment fence can owe
# (#5377). A force-settled ``cancelled`` request owes one too, by its own path.
TEARDOWN_TERMINAL_STATUSES = ("completed", "failed", "expired")
# A personal request ending with one of these is resumed by its owner after a
# replace or reconnect, so its WorkItem keeps its transcript (#5377). A personal
# request whose start deferrals ran out (``start_failed``, #5378) is resumed the
# same way, after reconnect and revalidation.
_RESUMABLE_PERSONAL_CAUSES = frozenset(
    {"enrollment_fenced", "authentication_required", "start_failed"}
)


@dataclass(frozen=True)
class RetryConditionFact:
    """The provider retry condition a worker carried to settlement (#5424).

    Already validated against the ACI grammar at the wire boundary; this is
    the typed fact the lifecycle persists and the policy reads.
    """

    reason: str
    retry_at: datetime | None
    scope_key: str | None


def automatic_retry_delay_seconds(work_item_id: uuid.UUID, slot: int) -> float:
    """The backoff before slot ``slot``'s successor, jittered by WorkItem identity.

    Keyed by the WorkItem and slot so a replayed settlement computes the same
    delay, and different WorkItems do not wake together.
    """

    base = AUTOMATIC_RETRY_DELAYS_S[slot - 1]
    digest = sha256(f"{work_item_id}:{slot}".encode()).digest()
    unit = int.from_bytes(digest[:8], "big") / 2**64
    return base * (1 - AUTOMATIC_RETRY_JITTER + 2 * AUTOMATIC_RETRY_JITTER * unit)


# The worker limits of a start that claimed no sandbox: nothing was observed.
NO_DELIVERED_LIMITS: Mapping[str, Any] = {
    "memory_bytes": None,
    "ephemeral_storage_bytes": None,
    "scratch_bytes": {},
    "max_turns": None,
}


_NO_READMIT: dict[str, Any] = {
    "readmit_request_id": None,
    "readmit_requester": None,
    "readmit_objective": None,
    "readmit_base_branch": None,
    "readmit_base_source": None,
    "readmit_base_commit": None,
    "readmit_base_settle_until": None,
}


def ready_base_transition_exists(request_id: Any = ExecutionRequest.id) -> ColumnElement[bool]:
    active_move = aliased(FactoryBaseTransition)
    return exists(
        select(FactoryBaseTransition.id)
        .join(WorkItem, WorkItem.id == FactoryBaseTransition.work_item_id)
        .join(
            ThreadPublicationLineage,
            ThreadPublicationLineage.id == FactoryBaseTransition.lineage_id,
        )
        .where(
            FactoryBaseTransition.ci_request_id == request_id,
            FactoryBaseTransition.phase == "applied",
            FactoryBaseTransition.outcome_ready_at.is_not(None),
            FactoryBaseTransition.ci_ready_at.is_not(None),
            FactoryBaseTransition.ci_settled_at.is_(None),
            WorkItem.cancelled_at.is_(None),
            WorkItem.publication_lineage_id == FactoryBaseTransition.lineage_id,
            WorkItem.base_resolved_request_id == FactoryBaseTransition.ci_request_id,
            ThreadPublicationLineage.head_sha == FactoryBaseTransition.verification_head_sha,
            ThreadPublicationLineage.base_ref == FactoryBaseTransition.target_base_ref,
            ThreadPublicationLineage.base_sha == FactoryBaseTransition.target_base_sha,
            WorkItem.base_branch == FactoryBaseTransition.target_base_ref,
            WorkItem.base_commit == FactoryBaseTransition.target_base_sha,
            ~exists().where(
                active_move.work_item_id == FactoryBaseTransition.work_item_id,
                or_(
                    active_move.phase.in_(
                        ("pending", "prepared", "pushed", "retargeted", "ci_prepared", "ci_pushed")
                    ),
                    and_(active_move.phase == "applied", active_move.outcome_ready_at.is_(None)),
                ),
                active_move.id != FactoryBaseTransition.id,
            ),
            ~exists().where(
                Publication.execution_request_id == FactoryBaseTransition.ci_request_id,
                Publication.status.in_(AWAITING_PUBLICATION),
            ),
        )
        .correlate(ExecutionRequest)
    )


def not_awaiting_publication() -> ColumnElement[bool]:
    return and_(
        ~exists().where(
            Publication.execution_request_id == ExecutionRequest.id,
            Publication.status.in_(AWAITING_PUBLICATION),
        ),
        ~ready_base_transition_exists(),
    )


async def _awaits_publication(session: AsyncSession, request_id: uuid.UUID) -> bool:
    return bool(
        await session.scalar(
            select(
                or_(
                    exists().where(
                        Publication.execution_request_id == request_id,
                        Publication.status.in_(AWAITING_PUBLICATION),
                    ),
                    ready_base_transition_exists(request_id),
                )
            )
        )
    )


async def _applied_base_transition(
    session: AsyncSession, work_item: WorkItem, request: ExecutionRequest, *, ready: bool
) -> FactoryBaseTransition | None:
    if work_item.publication_lineage_id is None or work_item.base_resolved_request_id != request.id:
        return None
    active_move = aliased(FactoryBaseTransition)
    transition: FactoryBaseTransition | None = await session.scalar(
        select(FactoryBaseTransition)
        .join(
            ThreadPublicationLineage,
            ThreadPublicationLineage.id == FactoryBaseTransition.lineage_id,
        )
        .where(
            FactoryBaseTransition.work_item_id == work_item.id,
            FactoryBaseTransition.ci_request_id == request.id,
            FactoryBaseTransition.lineage_id == work_item.publication_lineage_id,
            FactoryBaseTransition.phase == "applied",
            FactoryBaseTransition.outcome_ready_at.is_not(None),
            FactoryBaseTransition.ci_settled_at.is_(None),
            *((FactoryBaseTransition.ci_ready_at.is_not(None),) if ready else ()),
            ThreadPublicationLineage.head_sha == FactoryBaseTransition.verification_head_sha,
            ThreadPublicationLineage.base_ref == FactoryBaseTransition.target_base_ref,
            ThreadPublicationLineage.base_sha == FactoryBaseTransition.target_base_sha,
            FactoryBaseTransition.target_base_ref == work_item.base_branch,
            FactoryBaseTransition.target_base_sha == work_item.base_commit,
            ~exists().where(
                active_move.work_item_id == work_item.id,
                active_move.id != FactoryBaseTransition.id,
                or_(
                    active_move.phase.in_(
                        ("pending", "prepared", "pushed", "retargeted", "ci_prepared", "ci_pushed")
                    ),
                    and_(active_move.phase == "applied", active_move.outcome_ready_at.is_(None)),
                ),
            ),
        )
        .order_by(FactoryBaseTransition.applied_at.desc())
        .limit(1)
    )
    return transition


async def base_transition_round_offset(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    lineage_id: uuid.UUID | None,
) -> int:
    found = await session.scalar(
        select(FactoryBaseTransition.id)
        .where(
            FactoryBaseTransition.work_item_id == work_item_id,
            FactoryBaseTransition.ci_request_id == request_id,
            FactoryBaseTransition.lineage_id == lineage_id,
            FactoryBaseTransition.phase == "applied",
            FactoryBaseTransition.outcome_ready_at.is_not(None),
        )
        .limit(1)
    )
    return int(found is not None)


def _base_values(base: ResolvedBase | None) -> dict[str, Any]:
    """A freshly resolved base replaces the recorded one, or nothing changes."""

    if base is None:
        return {}
    return {
        "base_branch": base.branch,
        "base_source": base.source,
        "base_commit": base.commit,
        "base_label_ignored": None,
    }


def _pending_readmit_base(work_item: WorkItem) -> ResolvedBase | None:
    """The base stored beside a deferred relabel, when all three were stored."""

    branch = work_item.readmit_base_branch
    source = work_item.readmit_base_source
    commit = work_item.readmit_base_commit
    if branch is None or source is None or commit is None:
        return None
    from ..factory_base import ResolvedBase

    # The column check constraint is what limits the source; pass it through.
    return ResolvedBase(branch, cast(Literal["label", "default"], source), commit)


def work_item_identity_values(
    issue: TrackerIssueRef, repository: RepositoryRef
) -> dict[str, str | None]:
    """The WorkItem identity columns for one tracker issue and its repository."""

    return {
        "tracker_kind": issue.kind,
        "tracker_host": issue.host,
        "tracker_scope_id": issue.scope_id,
        "tracker_issue_id": issue.issue_id,
        "tracker_display_key": issue.display_key,
        "code_host_kind": repository.kind,
        "code_host_host": repository.host,
        "repository_project_id": repository.project_id,
        "repository_path": repository.path,
    }


def for_tracker_issue(issue: TrackerIssueRef) -> tuple[ColumnElement[bool], ...]:
    """The predicates selecting the one WorkItem keyed by ``issue``."""

    return (
        WorkItem.tracker_kind == issue.kind,
        WorkItem.tracker_host == issue.host,
        WorkItem.tracker_scope_id == issue.scope_id,
        WorkItem.tracker_issue_id == issue.issue_id,
    )


def same_repository(stored: RepositoryRef, other: RepositoryRef) -> bool:
    """Identity and path both match. `RepositoryRef` equality ignores the path."""

    return stored == other and stored.path == other.path


@dataclass(frozen=True)
class WorkItemSnapshot:
    id: uuid.UUID
    tracker_issue: TrackerIssueRef
    repository: RepositoryRef
    code_host_installation_id: int | None
    agent_id: uuid.UUID
    conversation_id: str
    publication_lineage_id: uuid.UUID | None
    cancelled_at: datetime | None
    version: int
    next_sequence: int
    # The owner tuple (#5376).
    tenant_id: uuid.UUID | None
    model_auth_mode: ModelAuthMode
    owner_principal_id: uuid.UUID | None
    enrollment_generation: int | None
    requester_principal_id: uuid.UUID | None
    coding_instructions: CodingInstructionsMetadata


@dataclass(frozen=True)
class ExecutionRequestSnapshot:
    id: uuid.UUID
    work_item_id: uuid.UUID
    sequence: int
    status: RequestStatus
    wait_deadline: datetime | None
    started_at: datetime | None
    execution_deadline: datetime | None
    terminal_at: datetime | None
    terminal_cause: str | None
    termination_observation: str | None
    version: int
    # The owner tuple this request snapshotted at insertion (#5376).
    tenant_id: uuid.UUID | None
    model_auth_mode: ModelAuthMode
    owner_principal_id: uuid.UUID | None
    enrollment_generation: int | None
    coding_instructions_selection_id: uuid.UUID | None
    coding_instructions_evidence: EvidenceState


@dataclass(frozen=True)
class WorkItemOutcome:
    work_item: WorkItemSnapshot
    request: ExecutionRequestSnapshot | None
    replayed: bool = False


@dataclass(frozen=True)
class WorkItemConflict:
    code: ConflictCode
    work_item_id: uuid.UUID | None = None
    request_id: uuid.UUID | None = None
    work_item_version: int | None = None
    request_version: int | None = None


WorkItemResult = WorkItemOutcome | WorkItemConflict


def _work_item_snapshot(row: WorkItem) -> WorkItemSnapshot:
    return WorkItemSnapshot(
        id=row.id,
        tracker_issue=row.tracker_issue,
        repository=row.repository,
        code_host_installation_id=row.code_host_installation_id,
        agent_id=row.agent_id,
        conversation_id=row.conversation_id,
        publication_lineage_id=row.publication_lineage_id,
        cancelled_at=row.cancelled_at,
        version=row.version,
        next_sequence=row.next_sequence,
        tenant_id=row.tenant_id,
        model_auth_mode=mode_of(row.model_auth_mode),
        owner_principal_id=row.owner_principal_id,
        enrollment_generation=row.enrollment_generation,
        requester_principal_id=row.requester_principal_id,
        coding_instructions=metadata(row),
    )


def _request_snapshot(row: ExecutionRequest) -> ExecutionRequestSnapshot:
    return ExecutionRequestSnapshot(
        id=row.id,
        work_item_id=row.work_item_id,
        sequence=row.sequence,
        status=cast(RequestStatus, row.status),
        wait_deadline=row.wait_deadline,
        started_at=row.started_at,
        execution_deadline=row.execution_deadline,
        terminal_at=row.terminal_at,
        terminal_cause=row.terminal_cause,
        termination_observation=row.termination_observation,
        version=row.version,
        tenant_id=row.tenant_id,
        model_auth_mode=mode_of(row.model_auth_mode),
        owner_principal_id=row.owner_principal_id,
        enrollment_generation=row.enrollment_generation,
        coding_instructions_selection_id=row.coding_instructions_selection_id,
        coding_instructions_evidence=evidence(row.coding_instructions_evidence),
    )


async def outcome(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest | None,
    *,
    replayed: bool = False,
) -> WorkItemOutcome:
    result = WorkItemOutcome(
        work_item=_work_item_snapshot(work_item),
        request=_request_snapshot(request) if request is not None else None,
        replayed=replayed,
    )
    await session.commit()
    return result


async def _conflict(
    session: AsyncSession,
    code: ConflictCode,
    *,
    work_item_id: uuid.UUID | None = None,
    request_id: uuid.UUID | None = None,
    work_item: WorkItem | None = None,
    request: ExecutionRequest | None = None,
) -> WorkItemConflict:
    result = WorkItemConflict(
        code=code,
        work_item_id=work_item.id if work_item is not None else work_item_id,
        request_id=request.id if request is not None else request_id,
        work_item_version=work_item.version if work_item is not None else None,
        request_version=request.version if request is not None else None,
    )
    await session.commit()
    return result


async def database_now(session: AsyncSession) -> datetime:
    value = await session.scalar(select(func.clock_timestamp()))
    return cast(datetime, value)


async def _queue_notice(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest,
    *,
    detail: str | None,
) -> None:
    """Stage the terminal result on the request's status comment row (#3077).

    The caller commits. The row normally exists from admission; a request
    admitted before it did gets one here. Reply routing is resolved when the
    comment is delivered. ``detail`` is the provider's own failure message
    (#3073); it is redacted before it is stored, then clipped, so no key or
    token reaches the row or the comment.
    """

    if request.terminal_at is None:
        return
    cause = request.terminal_cause
    if cause is None or not cause.strip():
        return
    stored = _notice_detail(detail)
    await session.execute(
        insert(FactoryStatusComment)
        .values(
            execution_request_id=request.id,
            work_item_id=work_item.id,
            card_token=new_card_token(),
            terminal_cause=cause.strip(),
            detail=stored,
        )
        .on_conflict_do_update(
            index_elements=["execution_request_id"],
            set_={"terminal_cause": cause.strip(), "detail": stored},
        )
    )


def _notice_detail(detail: str | None) -> str | None:
    # Redact before clip so a truncated key still matches the redactor.
    text = redact_text((detail or "").strip())
    if len(text) > _NOTICE_DETAIL_MAX:
        text = text[: _NOTICE_DETAIL_MAX - 3].rstrip() + "..."
    return text or None


async def settle_terminal(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest,
    *,
    detail: str | None,
) -> None:
    """Stage everything a terminal request owes in its own transaction.

    That is the factory comment and, per ADR-0170, deleting the thread's
    transcript: a terminal WorkItem's history is not resumed again. A queued
    revision, an active request (an owner_lost successor, ADR 0206) or a
    pending relabel continues the WorkItem, so its transcript survives. So
    does a personal request stopped by an enrollment fence or ending
    authentication-required on an uncancelled WorkItem (#5377, ADR 0268
    section 3): its owner resumes it after a replace or reconnect. A personal
    request that ended ``start_failed`` keeps it too (#5378).
    """

    if request.terminal_at is None:
        return
    await factory_questions.settle_round(session, work_item, request)
    await _queue_notice(session, work_item, request, detail=detail)
    continuing = await session.scalar(
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.status.in_(("queued", *_ACTIVE_STATUSES)),
        )
        .limit(1)
    )
    resumable = (
        request.model_auth_mode == "personal"
        and request.terminal_cause in _RESUMABLE_PERSONAL_CAUSES
        and work_item.cancelled_at is None
    )
    if continuing is None and work_item.readmit_request_id is None and not resumable:
        await transcripts.expire_for_work_item(session, work_item)


CancellationCause = Literal["issue_cancelled", "issue_deleted"]


async def _cancel_queued_requests(
    session: AsyncSession, work_item_id: uuid.UUID, now: datetime, cause: CancellationCause
) -> None:
    await session.execute(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.status == "queued",
        )
        .values(
            status="cancelled",
            terminal_at=now,
            terminal_cause=cause,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
    )


async def _opened_pull_request(
    session: AsyncSession, work_item: WorkItem, request: ExecutionRequest
) -> bool:
    if work_item.publication_lineage_id is None:
        return False
    pr_url = await session.scalar(
        select(ThreadPublicationLineage.pr_url).where(
            ThreadPublicationLineage.id == work_item.publication_lineage_id
        )
    )
    if not isinstance(pr_url, str) or not pr_url.strip():
        return False
    succeeded = await session.scalar(
        select(Publication.id).where(
            Publication.execution_request_id == request.id,
            Publication.lineage_id == work_item.publication_lineage_id,
            Publication.status == "succeeded",
        )
    )
    return (
        succeeded is not None
        or await _applied_base_transition(session, work_item, request, ready=True) is not None
    )


async def _publication_owns_terminus(
    session: AsyncSession, work_item: WorkItem, request: ExecutionRequest
) -> bool:
    """Whether a publication can still settle this request (#4158).

    This request's own publication, in any status, or one still in flight on
    the work item's lineage or conversation, owns the terminus. An earlier
    request's settled publication on the same lineage does not.
    """
    scope = Publication.workspace_conversation_id == work_item.conversation_id
    if work_item.publication_lineage_id is not None:
        scope = or_(scope, Publication.lineage_id == work_item.publication_lineage_id)
    found = await session.scalar(
        select(Publication.id)
        .where(
            or_(
                Publication.execution_request_id == request.id,
                and_(scope, Publication.status.in_(IN_FLIGHT_PUBLICATION)),
            )
        )
        .limit(1)
    )
    return (
        found is not None
        or await _applied_base_transition(session, work_item, request, ready=True) is not None
    )


async def lock_work_item(session: AsyncSession, work_item_id: uuid.UUID) -> WorkItem | None:
    row: WorkItem | None = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == work_item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def lock_request(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
) -> ExecutionRequest | None:
    row: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def lock_request_by_id(
    session: AsyncSession, request_id: uuid.UUID
) -> ExecutionRequest | None:
    row: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.id == request_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def lock_active_request(
    session: AsyncSession, work_item_id: uuid.UUID
) -> ExecutionRequest | None:
    row: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.status.in_(_ACTIVE_STATUSES),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return row


async def reload_work_item(session: AsyncSession, work_item_id: uuid.UUID) -> WorkItem:
    row = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == work_item_id)
        .execution_options(populate_existing=True)
    )
    assert row is not None
    return row


async def reload_request(session: AsyncSession, request_id: uuid.UUID) -> ExecutionRequest:
    row = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.id == request_id)
        .execution_options(populate_existing=True)
    )
    assert row is not None
    return row


async def create_or_get_work_item(
    session: AsyncSession,
    *,
    issue: TrackerIssueRef,
    repository: RepositoryRef,
    code_host_installation_id: int | None,
    agent_id: uuid.UUID,
    conversation_id: str,
    base: ResolvedBase | None = None,
    trigger_label: str | None,
    owner: OwnerTuple | None = None,
    requester_principal_id: uuid.UUID | None = None,
) -> WorkItemResult:
    """Insert the WorkItem, or return the existing one for the same issue.

    The issue is the key (ADR 0197). The repository and installation are
    frozen on insert; a replay must name the same ones. The base fields (ADR
    0186) are written on insert only. A replay does not compare them: the base
    an existing WorkItem recorded is the one it keeps. ``trigger_label`` is the
    label that admitted it (#4361 decision 10), None for an admission no label
    started; it follows the base fields' rule.

    ``owner`` is the validated owner tuple the WorkItem is bound to (#5376);
    None is a service WorkItem in the agent's tenant. An existing WorkItem
    keeps its tuple: an explicit personal owner must equal it, and anything
    else reuses it.
    """

    owner_values = (
        owner.columns()
        if owner is not None
        else {
            "tenant_id": select(Agent.tenant_id).where(Agent.id == agent_id).scalar_subquery(),
            "model_auth_mode": "service",
            "owner_principal_id": None,
            "enrollment_generation": None,
        }
    )
    current = await session.scalar(select(WorkItem.id).where(*for_tracker_issue(issue)))
    coding_values: dict[str, Any] = {}
    selected = None
    if current is None:
        coding_values, selected = await selection_columns(
            session, agent_id=agent_id, principal_id=requester_principal_id
        )
    new_id = uuid.uuid4()
    statement = (
        insert(WorkItem)
        .values(
            id=new_id,
            **owner_values,
            **coding_values,
            **work_item_identity_values(issue, repository),
            code_host_installation_id=code_host_installation_id,
            agent_id=agent_id,
            conversation_id=conversation_id,
            base_branch=None if base is None else base.branch,
            base_source=None if base is None else base.source,
            base_commit=None if base is None else base.commit,
            trigger_label=trigger_label,
            version=1,
            next_sequence=1,
        )
        .on_conflict_do_nothing(
            index_elements=[
                WorkItem.tracker_kind,
                WorkItem.tracker_host,
                WorkItem.tracker_scope_id,
                WorkItem.tracker_issue_id,
            ]
        )
        .returning(WorkItem.id)
    )
    try:
        async with session.begin_nested():
            inserted_id = await session.scalar(statement)
            if inserted_id is not None:
                await add_snapshot(session, work_item_id=inserted_id, selection=selected)
                await session.flush()
    except IntegrityError:
        return await _conflict(session, "identity_mismatch")

    work_item = await session.scalar(
        select(WorkItem)
        .where(*for_tracker_issue(issue))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if work_item is None:
        return await _conflict(session, "identity_mismatch")
    exact_replay = (
        work_item.code_host_installation_id == code_host_installation_id
        and work_item.agent_id == agent_id
        and same_repository(work_item.repository, repository)
        and work_item.conversation_id == conversation_id
    )
    if not exact_replay:
        return await _conflict(session, "identity_mismatch", work_item=work_item)
    if inserted_id is None and owner is not None and owner.is_personal:
        mismatch = compare_owner(OwnerTuple.of(work_item), owner)
        if mismatch is not None:
            return await _conflict(session, mismatch, work_item=work_item)
    return await outcome(session, work_item, None, replayed=inserted_id is None)


async def _insert_waiting_request(
    session: AsyncSession,
    work_item: WorkItem,
    *,
    request_id: uuid.UUID,
    wait_deadline: datetime,
    expected_work_item_version: int,
    columns: Mapping[str, Any] | None = None,
    base_settle_until: datetime | None = None,
    completion_contract: CompletionContractName = CONTRACT_IMPLEMENTATION,
    result_policy_digest: str | None = None,
    reserve_automatic_retry: bool,
) -> bool:
    """Allocate the next sequence and insert a waiting request; the caller commits.

    False when the WorkItem moved past ``expected_work_item_version`` or was
    cancelled, or, with ``reserve_automatic_retry``, when its automatic
    allowance is spent. An ``IntegrityError`` leaves the savepoint rolled
    back. The request snapshots ``work_item``'s owner tuple, so ``work_item``
    is the locked row and ``columns`` never names an owner fact (#5376). The
    completion contract and its policy digest are admission's selection
    (ADR 0277, #5586). The allowance slot is reserved in the same statement
    that bumps the version and sequence, so no successor exists without its
    slot (ADR 0269).
    """

    assert_no_owner_columns(columns or {})
    sequence = work_item.next_sequence
    allowance: tuple[ColumnElement[bool], ...] = ()
    reservation: dict[str, Any] = {}
    if reserve_automatic_retry:
        allowance = (WorkItem.automatic_retries_used < AUTOMATIC_RETRY_LIMIT,)
        reservation = {"automatic_retries_used": WorkItem.automatic_retries_used + 1}
    async with session.begin_nested():
        changed_id: uuid.UUID | None = await session.scalar(
            update(WorkItem)
            .where(
                WorkItem.id == work_item.id,
                WorkItem.version == expected_work_item_version,
                WorkItem.cancelled_at.is_(None),
                *allowance,
            )
            .values(
                version=WorkItem.version + 1,
                next_sequence=WorkItem.next_sequence + 1,
                updated_at=func.clock_timestamp(),
                **reservation,
            )
            .returning(WorkItem.id)
        )
        if changed_id is None:
            return False
        session.add(
            ExecutionRequest(
                id=request_id,
                work_item_id=work_item.id,
                sequence=sequence,
                status="waiting",
                wait_deadline=wait_deadline,
                base_settle_until=base_settle_until,
                version=1,
                completion_contract=completion_contract,
                result_policy_digest=result_policy_digest,
                **snapshot_columns(work_item),
                **request_selection_columns(work_item),
                **(columns or {}),
            )
        )
        # The live status comment's row exists from admission (#3077).
        session.add(
            FactoryStatusComment(
                execution_request_id=request_id,
                work_item_id=work_item.id,
                applied_label=None,
            )
        )
        await session.flush()
    return True


async def create_execution_request(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    wait_deadline: datetime,
    expected_work_item_version: int,
    base_settle_until: datetime | None = None,
    completion_contract: CompletionContractName = CONTRACT_IMPLEMENTATION,
    result_policy_digest: str | None = None,
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(session, "not_found", work_item_id=work_item_id)
    if work_item.cancelled_at is not None:
        return await _conflict(session, "work_item_cancelled", work_item=work_item)

    existing = await lock_request_by_id(session, request_id)
    if existing is not None:
        if existing.work_item_id != work_item_id or existing.wait_deadline != wait_deadline:
            return await _conflict(
                session, "identity_mismatch", work_item=work_item, request=existing
            )
        return await outcome(session, work_item, existing, replayed=True)

    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item)
    active = await lock_active_request(session, work_item_id)
    if active is not None:
        return await _conflict(session, "active_request", work_item=work_item, request=active)

    try:
        allocation_changed = await _insert_waiting_request(
            session,
            work_item,
            request_id=request_id,
            wait_deadline=wait_deadline,
            base_settle_until=base_settle_until,
            expected_work_item_version=expected_work_item_version,
            completion_contract=completion_contract,
            result_policy_digest=result_policy_digest,
            reserve_automatic_retry=False,
        )
    except IntegrityError:
        work_item = await reload_work_item(session, work_item_id)
        existing = await lock_request_by_id(session, request_id)
        if existing is not None:
            if existing.work_item_id == work_item_id and (
                existing.wait_deadline == wait_deadline or existing.status == "waiting"
            ):
                return await outcome(session, work_item, existing, replayed=True)
            return await _conflict(
                session, "identity_mismatch", work_item=work_item, request=existing
            )
        active = await lock_active_request(session, work_item_id)
        if active is not None:
            return await _conflict(session, "active_request", work_item=work_item, request=active)
        return await _conflict(session, "stale_version", work_item=work_item)

    if not allocation_changed:
        work_item = await reload_work_item(session, work_item_id)
        return await _conflict(session, "stale_version", work_item=work_item)

    work_item = await reload_work_item(session, work_item_id)
    request = await reload_request(session, request_id)
    return await outcome(session, work_item, request)


async def create_revision_request(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    wait_deadline: datetime,
    expected_work_item_version: int,
    snapshot: Mapping[str, str | None],
) -> WorkItemResult:
    """Store a mention in sequence without interrupting the live request."""

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(session, "not_found", work_item_id=work_item_id)
    if work_item.cancelled_at is not None:
        return await _conflict(session, "work_item_cancelled", work_item=work_item)
    existing = await lock_request_by_id(session, request_id)
    if existing is not None:
        return await outcome(session, work_item, existing, replayed=True)
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item)
    active = await lock_active_request(session, work_item_id)
    older_queued = await session.scalar(
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.status == "queued",
        )
        .limit(1)
    )
    if active is None and older_queued is None and work_item.readmit_request_id is None:
        return await create_execution_request(
            session,
            work_item_id=work_item_id,
            request_id=request_id,
            wait_deadline=wait_deadline,
            expected_work_item_version=expected_work_item_version,
        )
    assert_no_owner_columns(snapshot)
    sequence = work_item.next_sequence
    try:
        async with session.begin_nested():
            changed_id = await session.scalar(
                update(WorkItem)
                .where(
                    WorkItem.id == work_item_id,
                    WorkItem.version == expected_work_item_version,
                    WorkItem.cancelled_at.is_(None),
                )
                .values(
                    version=WorkItem.version + 1,
                    next_sequence=WorkItem.next_sequence + 1,
                    updated_at=func.clock_timestamp(),
                )
                .returning(WorkItem.id)
            )
            assert changed_id is not None
            session.add(
                ExecutionRequest(
                    id=request_id,
                    work_item_id=work_item_id,
                    sequence=sequence,
                    status="queued",
                    wait_deadline=None,
                    version=1,
                    **snapshot_columns(work_item),
                    **request_selection_columns(work_item),
                    **snapshot,
                )
            )
            session.add(
                FactoryStatusComment(
                    execution_request_id=request_id,
                    work_item_id=work_item_id,
                    applied_label=None,
                )
            )
            await session.flush()
    except IntegrityError:
        await session.rollback()
        return await _conflict(session, "stale_version", work_item_id=work_item_id)
    work_item = await reload_work_item(session, work_item_id)
    request = await reload_request(session, request_id)
    return await outcome(session, work_item, request)


async def _lineage_closed_for(
    session: AsyncSession, work_item: WorkItem, reply_target_pr_number: str | None
) -> bool:
    """Whether a revision's pull request is no longer the WorkItem's open lineage.

    A revision answers on its pull request, so it runs only while the lineage
    exists, is open, and carries that pull request's number.
    """

    if reply_target_pr_number is None:
        return False
    lineage = (
        await session.get(ThreadPublicationLineage, work_item.publication_lineage_id)
        if work_item.publication_lineage_id is not None
        else None
    )
    return (
        lineage is None
        or lineage.status != "open"
        or str(lineage.pr_number) != reply_target_pr_number
    )


def _has_no_change_marker(detail: str | None) -> bool:
    """Whether any line starts with the no-change marker.

    Leading whitespace on the line, then one leading run of blockquote (`>`)
    and emphasis (`*` and `_`) markers, is ignored. A marker later in the
    line does not count, and "No changes needed because" does not count.
    """

    if not detail:
        return False
    marker = "No changes needed:"
    for line in detail.splitlines():
        candidate = line.lstrip().lstrip(">*_").lstrip()
        if candidate.startswith(marker):
            return True
    return False


async def _unchanged_follow_up(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest,
    detail: str | None,
) -> bool:
    """An explicit no-change reply can complete a follow-up on its open PR."""

    if not _has_no_change_marker(detail):
        return False
    # The objective opens with the issue link admission wrote, so the
    # comparison reads the same link. An issue that cannot be linked (no
    # GitHub code host, or a binding that left config) is not a follow-up.
    try:
        link = issue_link(
            forges_registry(get_settings()), work_item.tracker_issue, work_item.repository
        )
    except ForgeError:
        return False
    prefix = comment_prefix(work_item.tracker_kind)
    follow_up = request.reply_target_kind != "issue" or is_issue_follow_up(
        request.objective, issue_link=link, comment_prefix=prefix
    )
    if not follow_up and not is_relabel_objective(
        request.objective, issue_link=link, comment_prefix=prefix
    ):
        return False
    lineage = (
        await session.get(ThreadPublicationLineage, work_item.publication_lineage_id)
        if work_item.publication_lineage_id is not None
        else None
    )
    if (
        lineage is None
        or lineage.status != "open"
        or not isinstance(lineage.pr_url, str)
        or not lineage.pr_url.strip()
    ):
        return False
    if not follow_up:
        # A relabel continues the PR only when an earlier request of this
        # WorkItem published on it (ADR 0208 consequence 4).
        adopted = await session.scalar(
            select(Publication.id)
            .join(ExecutionRequest, ExecutionRequest.id == Publication.execution_request_id)
            .where(
                Publication.lineage_id == lineage.id,
                Publication.status == "succeeded",
                ExecutionRequest.work_item_id == work_item.id,
                ExecutionRequest.sequence < request.sequence,
            )
            .limit(1)
        )
        if adopted is None:
            return False
    return (
        request.reply_target_pr_number is None
        or str(lineage.pr_number) == request.reply_target_pr_number
    )


async def admit_next_revision(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    wait_deadline: datetime,
) -> WorkItemResult | None:
    """Promote only the oldest queued mention after its predecessor ends."""

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None or work_item.readmit_request_id is not None:
        await session.commit()
        return None
    if await lock_active_request(session, work_item_id) is not None:
        await session.commit()
        return None
    request = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.status == "queued",
        )
        .order_by(ExecutionRequest.sequence)
        .limit(1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if request is None:
        await session.commit()
        return None
    now = await database_now(session)
    cause: str | None = None
    if work_item.cancelled_at is not None:
        cause = "issue_cancelled"
    elif await _lineage_closed_for(session, work_item, request.reply_target_pr_number):
        cause = "lineage_closed"
    if cause is not None:
        status = "cancelled"
        values: dict[str, Any] = {"terminal_at": now, "terminal_cause": cause}
    else:
        status = "waiting"
        values = {"wait_deadline": wait_deadline, "dispatch_not_before": now}
    await session.execute(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.version == request.version,
            ExecutionRequest.status == "queued",
        )
        .values(
            status=status,
            **values,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
    )
    await session.execute(
        update(WorkItem)
        .where(WorkItem.id == work_item_id)
        .values(version=WorkItem.version + 1, updated_at=func.clock_timestamp())
    )
    work_item = await reload_work_item(session, work_item_id)
    request = await reload_request(session, request.id)
    if cause is not None:
        await settle_terminal(session, work_item, request, detail=None)
    return await outcome(session, work_item, request)


async def start_execution_where(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    extra_where: Sequence[ColumnElement[bool]],
    extra_values: Mapping[str, Any],
    delivered_limits: Mapping[str, Any],
) -> WorkItemResult:
    """Start a waiting request.

    ``delivered_limits`` holds the limits the worker read from the sandbox it
    claimed for this attempt (#5427). The start records them with the two
    allowances the API enforces, and a gated request starts only when the
    record delivers strictly more of its gated dimension; otherwise the start
    is refused ``resource_gate_unmet`` and nothing changes.
    """

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item, request_id=request_id)
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "waiting":
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    deadline = request.wait_deadline
    if deadline is None:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)

    now = await database_now(session)
    if now >= deadline:
        return await _conflict(
            session, "waiting_deadline_elapsed", work_item=work_item, request=request
        )
    # Same transaction as the transition: the owning agent's operator override,
    # else the platform default (#3071).
    deadline_seconds = await execution_deadline_seconds(session, work_item.agent_id)
    record = resource_gate.delivered_record(
        delivered_limits,
        history_capacity_bytes=get_settings().transcript_max_thread_bytes,
        execution_seconds=deadline_seconds,
    )
    if request.resource_gate is not None and not resource_gate.gate_met(
        request.resource_gate, record
    ):
        return await _conflict(session, "resource_gate_unmet", work_item=work_item, request=request)
    values: dict[str, Any] = {
        "status": "running",
        "started_at": now,
        "execution_deadline": now + timedelta(seconds=deadline_seconds),
        "version": ExecutionRequest.version + 1,
        "updated_at": func.clock_timestamp(),
        # A started request is no longer failing preparation (#4930). Cleared
        # by this same CAS, so a refused or stale start leaves it intact.
        "last_start_failure_detail": None,
        # A gate is satisfied only by this start, so it clears with it.
        "blocked_on": None,
        **extra_values,
    }
    # The record names the epoch this same UPDATE writes, read from the row's
    # values as the statement assigns them.
    epoch = values.get("runtime_epoch", ExecutionRequest.runtime_epoch)
    values["delivered_limits"] = func.jsonb_build_object("runtime_epoch", epoch).op(
        "||", return_type=JSONB
    )(literal(record, type_=JSONB))
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "waiting",
            ExecutionRequest.wait_deadline > func.clock_timestamp(),
            *extra_where,
        )
        .values(**values)
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await reload_request(session, request_id)
        deadline = request.wait_deadline
        if (
            request.status == "waiting"
            and deadline is not None
            and await database_now(session) >= deadline
        ):
            return await _conflict(
                session,
                "waiting_deadline_elapsed",
                work_item=work_item,
                request=request,
            )
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await reload_request(session, request_id)
    await _bound_grant_to_deadline(session, request)
    return await outcome(session, work_item, request)


async def execution_deadline_seconds(session: AsyncSession, agent_id: uuid.UUID) -> int:
    """The agent's operator execution deadline override, else the platform default."""

    agent_deadline = await session.scalar(
        select(Agent.execution_deadline_seconds).where(Agent.id == agent_id)
    )
    return agent_deadline if agent_deadline is not None else DEFAULT_EXECUTION_DEADLINE_SECONDS


async def _bound_grant_to_deadline(session: AsyncSession, request: ExecutionRequest) -> None:
    """A started tracker result request's grant expires at its execution
    deadline plus the delivery deadline (ADR 0277 decision 4). The
    reservation minted it with a provisional expiry; the caller commits."""

    if request.completion_contract != CONTRACT_TRACKER_RESULT or request.execution_deadline is None:
        return
    delivery = timedelta(seconds=get_settings().factory_tracker_result_delivery_deadline_seconds)
    await session.execute(
        update(FactoryTrackerResultGrant)
        .where(FactoryTrackerResultGrant.execution_request_id == request.id)
        .values(expires_at=request.execution_deadline + delivery)
    )


async def _expire_unused_grants(session: AsyncSession, work_item_id: uuid.UUID) -> None:
    """Revoke the WorkItem's grants that delivered nothing: they expire now
    (ADR 0277 decision 4). The caller commits."""

    await session.execute(
        update(FactoryTrackerResultGrant)
        .where(
            FactoryTrackerResultGrant.work_item_id == work_item_id,
            FactoryTrackerResultGrant.expires_at > func.clock_timestamp(),
            ~exists().where(FactoryTrackerResultDelivery.grant_id == FactoryTrackerResultGrant.id),
        )
        .values(expires_at=func.clock_timestamp())
    )


TrackerResultSettlement = Literal["delivered", "none", "undelivered"]


async def _tracker_result_settlement(
    session: AsyncSession, request_id: uuid.UUID
) -> TrackerResultSettlement:
    """Whether the request's own submitted result is delivered with a receipt
    (``delivered``), was never submitted (``none``), or is owed, failed or not
    the run's own (``undelivered``)."""

    row = (
        await session.execute(
            select(
                FactoryTrackerResultDelivery.state,
                FactoryTrackerResultDelivery.origin,
                FactoryTrackerResultReceipt.delivery_id,
            )
            .outerjoin(
                FactoryTrackerResultReceipt,
                FactoryTrackerResultReceipt.delivery_id == FactoryTrackerResultDelivery.id,
            )
            .where(FactoryTrackerResultDelivery.execution_request_id == request_id)
            .limit(1)
        )
    ).one_or_none()
    if row is None:
        return "none"
    if (
        row.state == DeliveryState.DELIVERED.value
        and row.origin == DeliveryOrigin.SUBMITTED.value
        and row.delivery_id is not None
    ):
        return "delivered"
    return "undelivered"


async def start_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    return await start_execution_where(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        extra_where=(),
        extra_values={"execution_attempts": 1},
        # No worker claimed a sandbox for this start, so it observed no limit.
        delivered_limits=NO_DELIVERED_LIMITS,
    )


async def expire_waiting(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item, request_id=request_id)
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "waiting":
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    deadline = request.wait_deadline
    if deadline is None:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    now = await database_now(session)
    if now < deadline:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "waiting",
        )
        .values(
            status="expired",
            terminal_at=now,
            # A request blocked on a typed condition names that wait (ADR 0269);
            # blocked_on stays on the row so the condition is still readable.
            terminal_cause=(
                "retry_condition_expired"
                if request.blocked_on is not None
                else "capacity_wait_expired"
            ),
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await reload_request(session, request_id)
    await settle_terminal(session, work_item, request, detail=None)
    return await outcome(session, work_item, request)


ConditionReleaseRefusal = Literal[
    "not_found",
    "work_item_cancelled",
    "readmit_pending",
    "stale_version",
    "not_waiting",
    "already_started",
    "not_blocked",
    "waiting_deadline_elapsed",
    "condition_mismatch",
    "not_cleared",
]


@dataclass(frozen=True)
class ConditionRelease:
    """What one ``release_retry_condition`` call did.

    ``refusal`` is None when the block was lifted. ``condition`` names the
    block the request carried when it was read, when one was read.
    """

    request_id: uuid.UUID
    condition: str | None
    refusal: ConditionReleaseRefusal | None

    @property
    def released(self) -> bool:
        return self.refusal is None


async def _condition_release(
    session: AsyncSession,
    request_id: uuid.UUID,
    condition: str | None,
    refusal: ConditionReleaseRefusal | None,
) -> ConditionRelease:
    await session.commit()
    return ConditionRelease(request_id=request_id, condition=condition, refusal=refusal)


async def release_retry_condition(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    recheck: Callable[[], Awaitable[bool]],
) -> ConditionRelease:
    """Lift a verified provider block from a waiting automatic successor (#5426).

    In one transaction with the WorkItem and request locked: refuse a cancelled
    WorkItem, a pending relabel, a stale version, a request that is not
    waiting, already started, not blocked, or past its waiting deadline by
    database time, and a block that the factory policy no longer derives from
    the predecessor's persisted cause, safety fact and condition. Only then is
    ``recheck`` (a fresh read of the verifying evidence) awaited, and only its
    True clears ``blocked_on`` and makes the request due now, by a version
    compare-and-set. The allowance count, the waiting deadline, the
    predecessor and every other request are left alone, and the count is not
    consulted: admitting this successor is what spent its slot. A second call
    finds no block and changes nothing.
    """

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _condition_release(session, request_id, None, "not_found")
    if work_item.cancelled_at is not None:
        return await _condition_release(session, request_id, None, "work_item_cancelled")
    if work_item.readmit_request_id is not None:
        return await _condition_release(session, request_id, None, "readmit_pending")
    if work_item.version != expected_work_item_version:
        return await _condition_release(session, request_id, None, "stale_version")
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _condition_release(session, request_id, None, "not_found")
    condition = request.blocked_on
    if request.version != expected_request_version:
        return await _condition_release(session, request_id, condition, "stale_version")
    if request.status != "waiting":
        return await _condition_release(session, request_id, condition, "not_waiting")
    if request.started_at is not None:
        return await _condition_release(session, request_id, condition, "already_started")
    if condition is None:
        return await _condition_release(session, request_id, None, "not_blocked")
    deadline = request.wait_deadline
    if deadline is None or await database_now(session) >= deadline:
        return await _condition_release(session, request_id, condition, "waiting_deadline_elapsed")
    predecessor: ExecutionRequest | None = None
    if request.retry_of_request_id is not None:
        predecessor = await session.scalar(
            select(ExecutionRequest)
            .where(
                ExecutionRequest.id == request.retry_of_request_id,
                ExecutionRequest.work_item_id == work_item_id,
            )
            .execution_options(populate_existing=True)
        )
    if predecessor is None or predecessor.status != "failed":
        return await _condition_release(session, request_id, condition, "condition_mismatch")
    safety = predecessor.retry_safety
    disposition = factory_retry_policy.decide(
        RetryFacts(
            cause=predecessor.terminal_cause or "",
            safety=(
                cast(RetrySafety, safety)
                if safety in ("no_effects", "flagged", "uncertain")
                else None
            ),
            condition_reason=predecessor.retry_condition_reason,
            condition_retry_at=predecessor.retry_condition_retry_at,
            condition_scope_key=predecessor.retry_condition_scope_key,
        )
    )
    if not disposition.successor or disposition.condition != condition:
        return await _condition_release(session, request_id, condition, "condition_mismatch")
    if not await recheck():
        return await _condition_release(session, request_id, condition, "not_cleared")
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "waiting",
            ExecutionRequest.started_at.is_(None),
            ExecutionRequest.blocked_on == condition,
            ExecutionRequest.wait_deadline > func.clock_timestamp(),
        )
        .values(
            blocked_on=None,
            dispatch_not_before=func.clock_timestamp(),
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        return await _condition_release(session, request_id, condition, "stale_version")
    return await _condition_release(session, request_id, condition, None)


async def adopt_orphan_publication_lineage(
    session: AsyncSession,
    *,
    deployment_id: uuid.UUID,
    work_item_id: uuid.UUID,
    execution_request_id: uuid.UUID,
    runtime_epoch: int,
) -> None:
    """Commit an eligible predecessor's open PR link before any provider read.

    Lock in lifecycle order: WorkItem, calling request, then lineage. Publication
    provenance is read without locking its writer. Ineligible candidates leave
    the existing publication authority reader to return its usual refusal.
    """

    work_item = await lock_work_item(session, work_item_id)
    if (
        work_item is None
        or work_item.cancelled_at is not None
        or work_item.publication_lineage_id is not None
    ):
        await session.commit()
        return
    request = await lock_request(
        session, work_item_id=work_item_id, request_id=execution_request_id
    )
    deployment_agent_id = await session.scalar(
        select(Deployment.agent_id).where(Deployment.id == deployment_id)
    )
    now = await database_now(session)
    if (
        request is None
        or deployment_agent_id != work_item.agent_id
        or request.status != "running"
        or request.runtime_epoch != runtime_epoch
        or not request.runtime_owner
        or request.runtime_heartbeat_expires_at is None
        or request.runtime_heartbeat_expires_at <= now
        or request.execution_deadline is None
        or request.execution_deadline <= now
    ):
        await session.commit()
        return
    candidates = list(
        await session.scalars(
            select(ThreadPublicationLineage)
            .where(
                ThreadPublicationLineage.agent_id == work_item.agent_id,
                ThreadPublicationLineage.conversation_id == work_item.conversation_id,
                func.lower(ThreadPublicationLineage.repo_full_name)
                == work_item.repository_path.casefold(),
                ThreadPublicationLineage.status == "open",
                ThreadPublicationLineage.pr_number.is_not(None),
            )
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
    )
    if len(candidates) != 1:
        await session.commit()
        return
    lineage = candidates[0]
    if (
        lineage.repository != work_item.repository
        or lineage.code_host_installation_id != work_item.code_host_installation_id
    ):
        await session.commit()
        return
    owner = await session.scalar(
        select(WorkItem.id).where(WorkItem.publication_lineage_id == lineage.id)
    )
    predecessor_publication = await session.scalar(
        select(Publication.id)
        .join(ExecutionRequest, ExecutionRequest.id == Publication.execution_request_id)
        .where(
            Publication.lineage_id == lineage.id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.sequence < request.sequence,
        )
        .limit(1)
    )
    if owner is not None or predecessor_publication is None:
        await session.commit()
        return
    try:
        async with session.begin_nested():
            await session.execute(
                update(WorkItem)
                .where(
                    WorkItem.id == work_item_id,
                    WorkItem.version == work_item.version,
                    WorkItem.cancelled_at.is_(None),
                    WorkItem.publication_lineage_id.is_(None),
                    select(ExecutionRequest.id)
                    .where(
                        ExecutionRequest.id == execution_request_id,
                        ExecutionRequest.work_item_id == work_item_id,
                        ExecutionRequest.version == request.version,
                        ExecutionRequest.status == "running",
                        ExecutionRequest.runtime_epoch == runtime_epoch,
                        ExecutionRequest.runtime_heartbeat_expires_at > func.clock_timestamp(),
                        ExecutionRequest.execution_deadline > func.clock_timestamp(),
                    )
                    .exists(),
                )
                .values(
                    publication_lineage_id=lineage.id,
                    version=WorkItem.version + 1,
                    updated_at=func.clock_timestamp(),
                )
                .execution_options(synchronize_session=False)
            )
    except IntegrityError:
        # A competing work item can claim the unique link under a shared
        # lineage lock. Its committed ownership is handled by the reader.
        pass
    await session.commit()


async def link_publication_lineage(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    publication_lineage_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item, request_id=request_id)
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    running_request = request.status == "running"
    if running_request:
        now = await database_now(session)
        if request.execution_deadline is None or now >= request.execution_deadline:
            return await _conflict(
                session, "execution_deadline_elapsed", work_item=work_item, request=request
            )

    if (
        work_item.publication_lineage_id is not None
        and work_item.publication_lineage_id != publication_lineage_id
    ):
        return await _conflict(
            session, "lineage_already_owned", work_item=work_item, request=request
        )

    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == publication_lineage_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if not running_request and (
        lineage is None
        or lineage.status != "open"
        or lineage.pr_number is None
        or await session.scalar(
            select(Publication.id)
            .where(
                Publication.execution_request_id == request_id,
                Publication.lineage_id == publication_lineage_id,
                Publication.status == "succeeded",
            )
            .limit(1)
        )
        is None
    ):
        return await _conflict(
            session, "publication_ineligible", work_item=work_item, request=request
        )
    if lineage is None or (
        lineage.agent_id != work_item.agent_id
        or lineage.conversation_id != work_item.conversation_id
        or lineage.repo_full_name.casefold() != work_item.repository_path.casefold()
        or (
            (not running_request or lineage.repository is not None)
            and lineage.repository != work_item.repository
        )
        or (
            (not running_request or lineage.code_host_installation_id is not None)
            and lineage.code_host_installation_id != work_item.code_host_installation_id
        )
    ):
        return await _conflict(session, "lineage_mismatch", work_item=work_item, request=request)
    if work_item.publication_lineage_id == publication_lineage_id:
        return await outcome(session, work_item, request, replayed=True)
    owner = await session.scalar(
        select(WorkItem).where(WorkItem.publication_lineage_id == publication_lineage_id)
    )
    if owner is not None:
        return await _conflict(
            session, "lineage_already_owned", work_item=work_item, request=request
        )

    lineage_changed = False
    request_predicates: list[ColumnElement[bool]] = [
        ExecutionRequest.id == request_id,
        ExecutionRequest.work_item_id == work_item_id,
        ExecutionRequest.version == expected_request_version,
        ExecutionRequest.status == request.status,
    ]
    if running_request:
        request_predicates.append(ExecutionRequest.execution_deadline > func.clock_timestamp())
    try:
        async with session.begin_nested():
            changed_id: uuid.UUID | None = await session.scalar(
                update(WorkItem)
                .where(
                    WorkItem.id == work_item_id,
                    WorkItem.version == expected_work_item_version,
                    WorkItem.cancelled_at.is_(None),
                    WorkItem.publication_lineage_id.is_(None),
                    select(ExecutionRequest.id).where(*request_predicates).exists(),
                )
                .values(
                    publication_lineage_id=publication_lineage_id,
                    version=WorkItem.version + 1,
                    updated_at=func.clock_timestamp(),
                )
                .returning(WorkItem.id)
            )
            lineage_changed = changed_id is not None
    except IntegrityError:
        work_item = await reload_work_item(session, work_item_id)
        current_lineage = await session.scalar(
            select(ThreadPublicationLineage)
            .where(ThreadPublicationLineage.id == publication_lineage_id)
            .execution_options(populate_existing=True)
        )
        if current_lineage is None:
            return await _conflict(
                session, "lineage_mismatch", work_item=work_item, request=request
            )
        return await _conflict(
            session, "lineage_already_owned", work_item=work_item, request=request
        )
    if not lineage_changed:
        work_item = await reload_work_item(session, work_item_id)
        request = await reload_request(session, request_id)
        if running_request and (
            request.execution_deadline is None
            or await database_now(session) >= request.execution_deadline
        ):
            return await _conflict(
                session,
                "execution_deadline_elapsed",
                work_item=work_item,
                request=request,
            )
        if work_item.publication_lineage_id is not None:
            return await _conflict(
                session, "lineage_already_owned", work_item=work_item, request=request
            )
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    from ..factory_notices import mark_status_comment_stale

    await mark_status_comment_stale(session, work_item_id)
    work_item = await reload_work_item(session, work_item_id)
    return await outcome(session, work_item, request)


async def terminalize_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    status: Literal["completed", "failed"],
    cause: str,
    detail: str | None,
    ci_fix_round: int | None,
    extra_where: Sequence[ColumnElement[bool]],
    retry_safety: RetrySafety | None,
    retry_condition: RetryConditionFact | None,
    exhausted_volume: str | None,
) -> WorkItemResult:
    """Settle a running request completed or failed.

    ``retry_safety`` and ``retry_condition`` are the worker's typed facts
    (ADR 0269); a settlement the API makes itself passes None for both. They
    are written by the terminal UPDATE, and a failed request whose facts the
    factory policy allows may admit its automatic successor in this same
    transaction, before terminal settlement. ``exhausted_volume`` is the
    scratch volume a ``sandbox_scratch_exhausted`` finish named (#5427); the
    terminal UPDATE adds it to the request's delivered limits record, where
    the successor's gate reads it.
    """

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item, request_id=request_id)
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "running" or not cause.strip():
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    if cause.strip() == "ci_fix_unpublished" and ci_fix_round is None:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    tracker_result = request.completion_contract == CONTRACT_TRACKER_RESULT
    if not tracker_result and cause.strip() == CONTRACT_TRACKER_RESULT:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    now = await database_now(session)
    # A tracker result request completes only with its own delivered result's
    # receipt, never with a pull request (ADR 0277 decision 9). Without one it
    # fails: nothing was submitted, or the submission is still owed or failed.
    delivered = False
    if tracker_result and status == "completed":
        settlement = await _tracker_result_settlement(session, request.id)
        if settlement == "delivered":
            delivered, cause = True, "completed"
        else:
            status = "failed"
            cause = "no_tracker_result" if settlement == "none" else "tracker_result_undelivered"
    # The CI gate's causes (#3097) end a request whose pull request already
    # opened, so they share the opened-PR deadline exception. Keep this literal
    # equal to ``factory_ci.CI_CAUSES`` (importing it here would be circular).
    ci_cause = cause.strip() in {
        "ci_failed",
        "ci_timeout",
        "ci_unverified",
        "merge_conflict",
        "pr_closed",
    }
    # A delivered tracker result is the terminus the way an opened pull
    # request is, including after the deadline.
    opened = delivered or (
        not tracker_result
        and (status == "completed" or ci_cause)
        and await _opened_pull_request(session, work_item, request)
    )
    deadline_elapsed = request.execution_deadline is None or now >= request.execution_deadline
    # A pull request that already opened is the terminus even if reconciliation
    # notices it after the execution deadline.
    if deadline_elapsed and not opened:
        return await _conflict(
            session, "execution_deadline_elapsed", work_item=work_item, request=request
        )
    if status == "completed" and not opened:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    transition = await _applied_base_transition(session, work_item, request, ready=False)
    if (
        transition is not None
        and status == "failed"
        and cause.strip() in (*_UNPUBLISHED_CAUSES, "ci_fix_unpublished")
    ):
        publications = await session.scalar(
            select(func.count(Publication.id)).where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(AWAITING_PUBLICATION),
            )
        )
        if not publications and ci_fix_round is None:
            transition.ci_ready_at = now
            transition.version += 1
            return await _conflict(
                session, "publication_pending", work_item=work_item, request=request
            )
    if status == "failed" and cause.strip() == "ci_fix_unpublished":
        assert ci_fix_round is not None
        # A CI fix turn that ended without a new publication is terminal, unless
        # a publication of the request is still in flight, or the turn's own
        # round already published and awaits the CI gate's verdict; either one
        # settles the request. The gate dispatches round N when the request has
        # N - 1 succeeded publications and admits one turn per round, so the
        # Nth succeeded publication is round N's. Fewer than N succeeded means
        # this round never published, and the request fails at once.
        in_flight = await session.scalar(
            select(Publication.id)
            .where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(IN_FLIGHT_PUBLICATION),
            )
            .limit(1)
        )
        succeeded = await session.scalar(
            select(func.count(Publication.id)).where(
                Publication.execution_request_id == request.id,
                Publication.status == "succeeded",
            )
        )
        if (
            in_flight is not None
            or (succeeded or 0)
            + await base_transition_round_offset(
                session,
                work_item_id=work_item.id,
                request_id=request.id,
                lineage_id=work_item.publication_lineage_id,
            )
            >= ci_fix_round
        ):
            return await _conflict(
                session, "publication_pending", work_item=work_item, request=request
            )
    if status == "failed" and cause.strip() in _UNPUBLISHED_CAUSES and not tracker_result:
        if await _publication_owns_terminus(session, work_item, request):
            return await _conflict(
                session, "publication_pending", work_item=work_item, request=request
            )
        if await _unchanged_follow_up(session, work_item, request, detail):
            status = "completed"
            cause = "completed"
        elif (
            await factory_questions.question_ending(session, work_item.id, request.id)
            == factory_questions.QUESTIONS_ASKED
        ):
            # The question ending (#5785): a request whose posted questions
            # are this WorkItem's current round settles completed, so it never
            # spends an automatic failure successor and is never needs-human.
            status = "completed"
            cause = factory_questions.QUESTIONS_ASKED
    if status == "failed" and cause.strip() == "approval_create_failed":
        pending = await session.scalar(
            select(Publication.id)
            .where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(IN_FLIGHT_PUBLICATION),
            )
            .limit(1)
        )
        if pending is not None:
            return await _conflict(
                session, "publication_pending", work_item=work_item, request=request
            )
    if (
        status == "failed"
        and not tracker_result
        and await factory_questions.rounds_exhausted_for(session, work_item.id, request.id)
    ):
        # Refused past the round cap (#5785, decision 14): however this
        # request fails, it is terminal and needs a human.
        cause = factory_questions.QUESTION_ROUNDS_EXHAUSTED
    # The Python check above allows a pull request that already opened to
    # complete after the deadline. The UPDATE has to use the same exception,
    # or the deadline predicate rejects the row and the next pass cancels it.
    deadline_guard = (
        () if opened else (ExecutionRequest.execution_deadline > func.clock_timestamp(),)
    )
    # A request with no delivered record keeps none: concatenation with NULL
    # is NULL, so a named volume alone never makes a record a gate could use.
    volume_values: dict[str, Any] = (
        {}
        if exhausted_volume is None
        else {
            "delivered_limits": ExecutionRequest.delivered_limits.op("||", return_type=JSONB)(
                func.jsonb_build_object(resource_gate.EXHAUSTED_VOLUME_KEY, exhausted_volume)
            )
        }
    )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
            *deadline_guard,
            *extra_where,
        )
        .values(
            status=status,
            terminal_at=now,
            terminal_cause=cause.strip(),
            retry_safety=retry_safety,
            retry_condition_reason=None if retry_condition is None else retry_condition.reason,
            retry_condition_retry_at=None if retry_condition is None else retry_condition.retry_at,
            retry_condition_scope_key=(
                None if retry_condition is None else retry_condition.scope_key
            ),
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
            **volume_values,
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await reload_request(session, request_id)
        if not opened and (
            request.execution_deadline is None
            or await database_now(session) >= request.execution_deadline
        ):
            return await _conflict(
                session,
                "execution_deadline_elapsed",
                work_item=work_item,
                request=request,
            )
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await reload_request(session, request_id)
    if tracker_result and status == "failed" and cause.strip() == "budget_exceeded":
        # The session's dollar allowance ran out: the status stays failed
        # budget_exceeded, and the platform owes its findings (decision 10).
        await seal_platform_findings(
            session, request=request, work_item=work_item, limit="budget", settings=get_settings()
        )
    if request.status == "failed":
        disposition = factory_retry_policy.decide(
            RetryFacts(
                cause=request.terminal_cause or "",
                safety=retry_safety,
                condition_reason=None if retry_condition is None else retry_condition.reason,
                condition_retry_at=None if retry_condition is None else retry_condition.retry_at,
                condition_scope_key=(
                    None if retry_condition is None else retry_condition.scope_key
                ),
            )
        )
        # Before settlement, so the successor keeps the transcript (ADR 0269).
        work_item = await reload_work_item(session, work_item_id)
        if await admit_automatic_successor(
            session, work_item, request, origin="ordinary", disposition=disposition
        ):
            work_item = await reload_work_item(session, work_item_id)
    await settle_terminal(session, work_item, request, detail=detail)
    return await outcome(session, work_item, request)


async def complete_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    retry_safety: RetrySafety | None,
    retry_condition: RetryConditionFact | None,
) -> WorkItemResult:
    return await terminalize_execution(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        status="completed",
        cause="completed",
        detail=None,
        ci_fix_round=None,
        extra_where=(),
        retry_safety=retry_safety,
        retry_condition=retry_condition,
        exhausted_volume=None,
    )


async def fail_execution(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    cause: str,
    expected_work_item_version: int,
    expected_request_version: int,
    retry_safety: RetrySafety | None,
    retry_condition: RetryConditionFact | None,
) -> WorkItemResult:
    return await terminalize_execution(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        status="failed",
        cause=cause,
        detail=None,
        ci_fix_round=None,
        extra_where=(),
        retry_safety=retry_safety,
        retry_condition=retry_condition,
        exhausted_volume=None,
    )


@dataclass(frozen=True)
class PublicationCISubject:
    id: uuid.UUID
    version: int
    head_sha: str
    base_ref: str | None
    changed_paths: tuple[str, ...]
    published_at: datetime | None
    kind: Literal["publication"] = "publication"


@dataclass(frozen=True)
class BaseTransitionCISubject:
    id: uuid.UUID
    version: int
    head_sha: str
    base_ref: str | None
    changed_paths: tuple[str, ...]
    published_at: datetime | None
    kind: Literal["base_transition"] = "base_transition"


CISubject = PublicationCISubject | BaseTransitionCISubject


async def _ci_fence_holds(
    session: AsyncSession, work_item: WorkItem, request_id: uuid.UUID, subject: CISubject
) -> bool:
    if work_item.publication_lineage_id is None:
        return False
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == work_item.publication_lineage_id)
        .with_for_update(read=True)
    )
    if (
        lineage is None
        or lineage.head_sha != subject.head_sha
        or lineage.base_ref != subject.base_ref
    ):
        return False
    active = await session.scalar(
        select(Publication.id)
        .where(
            Publication.execution_request_id == request_id,
            Publication.status.in_(IN_FLIGHT_PUBLICATION),
        )
        .limit(1)
    )
    moving = await session.scalar(
        select(FactoryBaseTransition.id)
        .where(
            FactoryBaseTransition.work_item_id == work_item.id,
            FactoryBaseTransition.phase.not_in(("applied", "conflict", "refused")),
        )
        .limit(1)
    )
    if active is not None or moving is not None:
        return False
    latest = await session.scalar(
        select(Publication)
        .where(
            Publication.execution_request_id == request_id,
            Publication.status == "succeeded",
        )
        .order_by(Publication.revision_number.desc())
        .limit(1)
    )
    if isinstance(subject, PublicationCISubject):
        return latest is not None and latest.id == subject.id and latest.version == subject.version
    if latest is not None:
        return False
    request = await session.get(ExecutionRequest, request_id)
    transition = (
        await _applied_base_transition(session, work_item, request, ready=True) if request else None
    )
    return (
        transition is not None
        and transition.id == subject.id
        and transition.version == subject.version
    )


async def settle_ci_verdict(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
    expected_subject: CISubject,
    status: Literal["completed", "failed"],
    cause: str,
    detail: str | None,
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    request = (
        await lock_request(session, work_item_id=work_item_id, request_id=request_id)
        if work_item
        else None
    )
    if (
        work_item is not None
        and request is not None
        and not await _ci_fence_holds(session, work_item, request_id, expected_subject)
    ):
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if isinstance(expected_subject, BaseTransitionCISubject):
        async with AsyncSession(
            bind=await session.connection(),
            join_transaction_mode="create_savepoint",
            expire_on_commit=False,
        ) as settlement:
            result = await terminalize_execution(
                settlement,
                work_item_id=work_item_id,
                request_id=request_id,
                expected_work_item_version=expected_work_item_version,
                expected_request_version=expected_request_version,
                status=status,
                cause=cause,
                detail=detail,
                ci_fix_round=None,
                extra_where=(),
                retry_safety=None,
                retry_condition=None,
                exhausted_volume=None,
            )
        if isinstance(result, WorkItemOutcome):
            await session.execute(
                update(FactoryBaseTransition)
                .where(
                    FactoryBaseTransition.id == expected_subject.id,
                    FactoryBaseTransition.version == expected_subject.version,
                    FactoryBaseTransition.ci_request_id == request_id,
                )
                .values(
                    ci_settled_at=func.clock_timestamp(), version=FactoryBaseTransition.version + 1
                )
            )
        await session.commit()
        return result
    return await terminalize_execution(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        status=status,
        cause=cause,
        detail=detail,
        ci_fix_round=None,
        extra_where=(),
        retry_safety=None,
        retry_condition=None,
        exhausted_volume=None,
    )


async def hold_for_ci_fix(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_request_version: int,
    expected_subject: CISubject,
) -> bool:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        await session.commit()
        return False
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if (
        request is None
        or request.version != expected_request_version
        or request.status != "running"
        or not await _ci_fence_holds(session, work_item, request_id, expected_subject)
    ):
        await session.commit()
        return False
    changed = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
            ExecutionRequest.execution_deadline > func.clock_timestamp(),
        )
        .values(
            runtime_heartbeat_expires_at=ExecutionRequest.execution_deadline,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    await session.commit()
    return changed is not None


def _release_parked_hold() -> ColumnElement[datetime]:
    # hold_for_approval / hold_for_ci_fix park the lease at the execution
    # deadline so owner loss cannot reclaim waiting work. An explicit stop must
    # let any worker claim termination (#4627), so expire a parked hold now. An
    # ordinary short live lease is kept so claim_termination still fences a
    # live owner.
    return case(
        (
            ExecutionRequest.runtime_heartbeat_expires_at == ExecutionRequest.execution_deadline,
            func.clock_timestamp(),
        ),
        else_=ExecutionRequest.runtime_heartbeat_expires_at,
    )


async def _revoke_triage_delegations(
    session: AsyncSession, work_item_id: uuid.UUID, now: datetime
) -> None:
    """Cancelling a triage source revokes its unconsumed delegations for good (#5588).

    A later readmit clears ``cancelled_at`` but never this, so an older
    handoff cannot be revived by relabelling the source.
    """

    await session.execute(
        update(FactoryTriageDelegation)
        .where(
            FactoryTriageDelegation.work_item_id == work_item_id,
            FactoryTriageDelegation.consumed_at.is_(None),
            FactoryTriageDelegation.revoked_at.is_(None),
        )
        .values(revoked_at=now)
    )


async def request_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    expected_work_item_version: int,
    cause: CancellationCause = "issue_cancelled",
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(session, "not_found", work_item_id=work_item_id)
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item)
    if work_item.cancelled_at is not None:
        active = await lock_active_request(session, work_item_id)
        await _cancel_queued_requests(session, work_item_id, await database_now(session), cause)
        if work_item.readmit_request_id is None:
            return await outcome(session, work_item, active, replayed=True)
        # The last label action wins: an unlabel drops a pending relabel.
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == work_item_id)
            .values(
                **_NO_READMIT,
                version=WorkItem.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        work_item = await reload_work_item(session, work_item_id)
        return await outcome(session, work_item, active)

    now = await database_now(session)
    changed_id: uuid.UUID | None = await session.scalar(
        update(WorkItem)
        .where(
            WorkItem.id == work_item_id,
            WorkItem.version == expected_work_item_version,
            WorkItem.cancelled_at.is_(None),
        )
        .values(
            cancelled_at=now,
            **_NO_READMIT,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(WorkItem.id)
    )
    if changed_id is None:
        work_item = await reload_work_item(session, work_item_id)
        return await _conflict(session, "stale_version", work_item=work_item)

    await _cancel_queued_requests(session, work_item_id, now, cause)
    await _expire_unused_grants(session, work_item_id)
    # The WorkItem's question ends with it (#5785, decision 15): the row is
    # deleted, so no comment is an answer; the sweep above cancelled any
    # queued answer follow-up. The lifetime round count stays.
    await factory_questions.delete_for_cancellation(session, work_item_id)
    await _revoke_triage_delegations(session, work_item_id, now)

    active = await lock_active_request(session, work_item_id)
    if active is not None and active.status == "waiting":
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.version == active.version,
                ExecutionRequest.status == "waiting",
            )
            .values(
                status="cancelled",
                terminal_at=now,
                terminal_cause=cause,
                version=ExecutionRequest.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        active = await reload_request(session, active.id)
        await settle_terminal(session, work_item, active, detail=None)
    elif active is not None and active.status == "running":
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.version == active.version,
                ExecutionRequest.status == "running",
            )
            .values(
                status="cancellation_requested",
                terminal_cause=cause,
                cancellation_requested_at=now,
                runtime_heartbeat_expires_at=_release_parked_hold(),
                version=ExecutionRequest.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        active = await reload_request(session, active.id)
    elif (
        active is not None
        and active.status == "cancellation_requested"
        and active.terminal_cause in ("execution_deadline", "owner_lost", "enrollment_fenced")
    ):
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.version == active.version,
                ExecutionRequest.status == "cancellation_requested",
            )
            .values(
                terminal_cause=cause,
                version=ExecutionRequest.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        active = await reload_request(session, active.id)
    work_item = await reload_work_item(session, work_item_id)
    if active is None:
        # Nothing is left to run, so the cancelled WorkItem is terminal now. A
        # waiting or running request expires the transcript when it settles.
        await transcripts.expire_for_work_item(session, work_item)
    return await outcome(session, work_item, active)


async def readmit(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    wait_deadline: datetime,
    objective: str,
    requester: str,
    base: ResolvedBase | None = None,
    trigger_label: str | None,
    base_settle_until: datetime | None = None,
) -> WorkItemResult:
    """Start a new run on an existing WorkItem because the label was added again.

    A request created now gets its completion contract from admission's
    snapshot write; a relabel deferred behind a stopping run selects its
    contract when `admit_pending_readmit` creates it (ADR 0277, #5586).

    A waiting request is superseded in the same transaction. A running request
    is asked to stop, and the relabel is stored on the WorkItem until that
    request reaches a terminus; the returned request is then the old one. A
    freshly resolved base is recorded with the request that runs on it: now,
    or stored beside the deferred relabel so the stopping run keeps its own.
    ``trigger_label``, the label that readmitted it, replaces the stored one at
    once either way (#4361 decision 10): removing it is what cancels the
    relabel, deferred or not.
    """

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(session, "not_found", work_item_id=work_item_id)
    active = await lock_active_request(session, work_item_id)
    # The same timeline event can arrive twice before either caller has
    # committed its request row. Superseding that row would cancel the run
    # the other caller just admitted.
    if (
        active is not None and active.id == request_id
    ) or work_item.readmit_request_id == request_id:
        predecessor = active or await session.scalar(
            select(ExecutionRequest)
            .where(ExecutionRequest.work_item_id == work_item_id)
            .order_by(ExecutionRequest.sequence.desc())
            .limit(1)
        )
        return await outcome(session, work_item, predecessor, replayed=True)
    now = await database_now(session)
    moving = await session.scalar(
        select(FactoryBaseTransition)
        .where(
            FactoryBaseTransition.work_item_id == work_item_id,
            or_(
                FactoryBaseTransition.phase.in_(
                    ("pending", "prepared", "pushed", "retargeted", "ci_prepared", "ci_pushed")
                ),
                and_(
                    FactoryBaseTransition.phase == "applied",
                    FactoryBaseTransition.outcome_ready_at.is_(None),
                ),
            ),
        )
        .with_for_update()
        .limit(1)
    )
    if moving is not None and moving.phase != "pending":
        work_item.cancelled_at = None
        work_item.readmit_request_id, work_item.readmit_requester, work_item.readmit_objective = (
            request_id,
            requester,
            objective,
        )
        work_item.readmit_base_settle_until = base_settle_until
        work_item.trigger_label = trigger_label
        work_item.version += 1
        predecessor = active or await lock_request(
            session, work_item_id=work_item_id, request_id=moving.request_id
        )
        return await outcome(session, work_item, predecessor, replayed=False)
    if moving is not None:
        moving.phase, moving.error_code = "refused", "selection_superseded"
        moving.owner, moving.lease_expires_at = None, None
        moving.version += 1
    if active is not None and active.status in ("running", "cancellation_requested"):
        if active.status == "running":
            await session.execute(
                update(ExecutionRequest)
                .where(
                    ExecutionRequest.id == active.id,
                    ExecutionRequest.version == active.version,
                    ExecutionRequest.status == "running",
                )
                .values(
                    status="cancellation_requested",
                    terminal_cause="issue_cancelled",
                    cancellation_requested_at=now,
                    runtime_heartbeat_expires_at=_release_parked_hold(),
                    version=ExecutionRequest.version + 1,
                    updated_at=func.clock_timestamp(),
                )
            )
        elif active.terminal_cause != "issue_cancelled":
            await session.execute(
                update(ExecutionRequest)
                .where(
                    ExecutionRequest.id == active.id,
                    ExecutionRequest.version == active.version,
                    ExecutionRequest.status == "cancellation_requested",
                )
                .values(
                    terminal_cause="issue_cancelled",
                    version=ExecutionRequest.version + 1,
                    updated_at=func.clock_timestamp(),
                )
            )
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == work_item_id)
            .values(
                readmit_request_id=request_id,
                readmit_requester=requester,
                readmit_objective=objective,
                readmit_base_branch=None if base is None else base.branch,
                readmit_base_source=None if base is None else base.source,
                readmit_base_commit=None if base is None else base.commit,
                readmit_base_settle_until=base_settle_until,
                trigger_label=trigger_label,
                version=WorkItem.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        work_item = await reload_work_item(session, work_item_id)
        active = await reload_request(session, active.id)
        return await outcome(session, work_item, active)
    if active is not None:
        # Superseded, not stopped: the new request speaks for the issue. The
        # old run's status comment is still finalized, with the superseded
        # text. Not settle_terminal: the WorkItem continues, so its
        # transcript must survive.
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == active.id,
                ExecutionRequest.version == active.version,
                ExecutionRequest.status == "waiting",
            )
            .values(
                status="cancelled",
                terminal_at=now,
                terminal_cause="issue_cancelled",
                acquire_owner=None,
                acquire_expires_at=None,
                acquired_generation=None,
                version=ExecutionRequest.version + 1,
                updated_at=func.clock_timestamp(),
            )
        )
        superseded = await reload_request(session, active.id)
        await _queue_notice(session, work_item, superseded, detail=None)
        await factory_questions.settle_round(session, work_item, superseded)
    version: int | None = await session.scalar(
        update(WorkItem)
        .where(WorkItem.id == work_item_id)
        .values(
            cancelled_at=None,
            **_NO_READMIT,
            **_base_values(base),
            trigger_label=trigger_label,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(WorkItem.version)
    )
    assert version is not None
    return await create_execution_request(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        wait_deadline=wait_deadline,
        expected_work_item_version=version,
        base_settle_until=base_settle_until,
    )


async def admit_pending_readmit(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    wait_deadline: datetime,
) -> WorkItemResult | None:
    """Create the request a relabel stored while the previous run was stopping.

    Returns None when there is nothing to admit yet.
    """

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None or work_item.readmit_request_id is None:
        await session.commit()
        return None
    active = await lock_active_request(session, work_item_id)
    if active is not None and active.status != "waiting":
        await session.commit()
        return None
    moving = await session.scalar(
        select(FactoryBaseTransition.id)
        .where(
            FactoryBaseTransition.work_item_id == work_item_id,
            or_(
                FactoryBaseTransition.phase.in_(
                    ("pending", "prepared", "pushed", "retargeted", "ci_prepared", "ci_pushed")
                ),
                and_(
                    FactoryBaseTransition.phase == "applied",
                    FactoryBaseTransition.outcome_ready_at.is_(None),
                ),
            ),
        )
        .limit(1)
    )
    if moving is not None:
        await session.commit()
        return None
    request_id = work_item.readmit_request_id
    reply_kind, reply_address, reply_conversation_id = reply_route(
        forges_registry(get_settings()), work_item.tracker_issue, work_item.repository_path
    )
    snapshot: dict[str, Any] = {
        "base_settle_until": work_item.readmit_base_settle_until,
        "objective": work_item.readmit_objective,
        "requester": work_item.readmit_requester,
        "reply_kind": reply_kind,
        "reply_address": reply_address,
        "reply_conversation_id": reply_conversation_id,
    }
    if snapshot["objective"] is None or snapshot["requester"] is None:
        # Never commit a request that cannot be dispatched.
        await session.rollback()
        return None
    # The contract is selected now, from the binding's in-force version, as
    # admission selects it (ADR 0277, #5586). A refusal is a label no binding
    # serves: the stored relabel is dropped.
    contract = await select_completion_contract(
        session,
        agent_id=work_item.agent_id,
        tracker_kind=work_item.tracker_kind,
        tracker_host=work_item.tracker_host,
        registry=forges_registry(get_settings()),
    )
    if isinstance(contract, ContractRefused):
        await session.execute(
            update(WorkItem)
            .where(WorkItem.id == work_item_id)
            .values(**_NO_READMIT, version=WorkItem.version + 1, updated_at=func.clock_timestamp())
        )
        await session.commit()
        return None
    if active is not None:
        active.status, active.terminal_cause = "cancelled", "issue_cancelled"
        active.terminal_at = await database_now(session)
        active.acquire_owner, active.acquire_expires_at, active.acquired_generation = (
            None,
            None,
            None,
        )
        active.version += 1
        await session.flush()
        await _queue_notice(session, work_item, active, detail=None)
        await factory_questions.settle_round(session, work_item, active)
    version: int | None = await session.scalar(
        update(WorkItem)
        .where(WorkItem.id == work_item_id)
        .values(
            cancelled_at=None,
            **_NO_READMIT,
            **_base_values(_pending_readmit_base(work_item)),
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(WorkItem.version)
    )
    assert version is not None
    result = await create_execution_request(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        wait_deadline=wait_deadline,
        expected_work_item_version=version,
        base_settle_until=snapshot["base_settle_until"],
        completion_contract=contract.contract,
        result_policy_digest=contract.result_policy_digest,
    )
    if isinstance(result, WorkItemOutcome):
        await session.execute(
            update(ExecutionRequest)
            .where(
                ExecutionRequest.id == request_id,
                ExecutionRequest.objective.is_(None),
            )
            .values(**snapshot, updated_at=func.clock_timestamp())
        )
        await session.commit()
    return result


async def settle_overdue_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_request_version: int,
    settle_seconds: int,
) -> WorkItemResult:
    """Settle an issue cancellation no worker has confirmed within the window.

    The runtime epoch advances and the owner is cleared, so a late teardown
    receipt from the old owner is refused as stale.
    """

    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    window = timedelta(seconds=settle_seconds)
    now = await database_now(session)
    if (
        request.status != "cancellation_requested"
        or request.terminal_cause not in ("issue_cancelled", "issue_deleted")
        or request.cancellation_requested_at is None
        or request.cancellation_requested_at + window > now
        or request.terminate_published_at is None
        or not (
            request.runtime_owner is None
            or (
                request.runtime_heartbeat_expires_at is not None
                and request.runtime_heartbeat_expires_at <= now
            )
        )
    ):
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "cancellation_requested",
            ExecutionRequest.terminal_cause.in_(("issue_cancelled", "issue_deleted")),
            ExecutionRequest.terminate_published_at.is_not(None),
            ExecutionRequest.cancellation_requested_at <= now - window,
            ExecutionRequest.runtime_owner.is_(None)
            | (
                ExecutionRequest.runtime_heartbeat_expires_at.is_not(None)
                & (ExecutionRequest.runtime_heartbeat_expires_at <= now)
            ),
        )
        .values(
            status="cancelled",
            terminal_at=now,
            termination_observation=(
                f"settled by the control plane after {settle_seconds}s "
                "without a worker teardown receipt"
            ),
            teardown_unconfirmed_at=now,
            runtime_epoch=ExecutionRequest.runtime_epoch + 1,
            runtime_owner=None,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await reload_request(session, request_id)
    await settle_terminal(session, work_item, request, detail=None)
    return await outcome(session, work_item, request)


async def request_execution_deadline_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item, request_id=request_id)
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "running":
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    now = await database_now(session)
    if request.execution_deadline is None or now < request.execution_deadline:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
        )
        .values(
            status="cancellation_requested",
            terminal_cause="execution_deadline",
            cancellation_requested_at=now,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await reload_request(session, request_id)
    return await outcome(session, work_item, request)


async def request_owner_lost_cancellation(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.cancelled_at is not None:
        return await _conflict(
            session, "work_item_cancelled", work_item=work_item, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item, request_id=request_id)
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if request.status != "running":
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    now = await database_now(session)
    ttl = timedelta(seconds=get_settings().work_item_runtime_ttl_seconds)
    heartbeat_lapsed = (
        request.runtime_heartbeat_expires_at is not None
        and request.runtime_heartbeat_expires_at + ttl <= now
    )
    owner_absent = (
        request.runtime_owner is None
        and request.started_at is not None
        and request.started_at + ttl <= now
    )
    if not heartbeat_lapsed and not owner_absent:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    if await _awaits_publication(session, request.id):
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "running",
            not_awaiting_publication(),
            (
                (
                    ExecutionRequest.runtime_heartbeat_expires_at.is_not(None)
                    & (
                        ExecutionRequest.runtime_heartbeat_expires_at
                        <= func.clock_timestamp() - ttl
                    )
                )
                | (
                    ExecutionRequest.runtime_owner.is_(None)
                    & ExecutionRequest.started_at.is_not(None)
                    & (ExecutionRequest.started_at <= func.clock_timestamp() - ttl)
                )
            ),
        )
        .values(
            status="cancellation_requested",
            terminal_cause="owner_lost",
            cancellation_requested_at=now,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await reload_request(session, request_id)
    return await outcome(session, work_item, request)


@dataclass(frozen=True)
class EnrollmentFence:
    """The requests one enrollment fence changed (#5377), by id."""

    stopping: tuple[uuid.UUID, ...]
    pending_teardown: tuple[uuid.UUID, ...]


async def fence_enrollment_generation(
    session: AsyncSession, *, tenant_id: uuid.UUID, owner_principal_id: uuid.UUID
) -> EnrollmentFence:
    """Fence every personal request of one owner after a replace or disconnect.

    ADR 0268 section 3. Runs in the caller's transaction, which already holds
    the owner's WorkItems and requests locked and owns the commit; nothing here
    commits. Every existing request of the owner ran under a generation at or
    below the enrollment's, so each is superseded:

    1. A ``running`` request becomes ``cancellation_requested`` with cause
       ``enrollment_fenced``; a parked approval or CI hold is released so any
       worker can claim the termination. It ends ``failed`` once a worker
       records the teardown.
    2. A started ``completed``, ``failed`` or ``expired`` request with no
       termination observation is marked ``teardown_unconfirmed_at``, so
       terminate wakes run until a worker records its runtime gone.
    3. ``cancellation_requested`` requests already end with an observation,
       and ``queued`` and ``waiting`` ones are fenced by their generation, so
       neither changes. The WorkItem is never cancelled.
    """

    owner = (
        ExecutionRequest.tenant_id == tenant_id,
        ExecutionRequest.owner_principal_id == owner_principal_id,
        ExecutionRequest.model_auth_mode == "personal",
    )
    stopping = await session.scalars(
        update(ExecutionRequest)
        .where(*owner, ExecutionRequest.status == "running")
        .values(
            status="cancellation_requested",
            terminal_cause="enrollment_fenced",
            cancellation_requested_at=func.clock_timestamp(),
            runtime_heartbeat_expires_at=_release_parked_hold(),
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    stopped = tuple(stopping.all())
    pending = await session.scalars(
        update(ExecutionRequest)
        .where(
            *owner,
            ExecutionRequest.status.in_(TEARDOWN_TERMINAL_STATUSES),
            ExecutionRequest.started_at.is_not(None),
            or_(
                ExecutionRequest.termination_observation.is_(None),
                func.btrim(ExecutionRequest.termination_observation) == "",
            ),
            ExecutionRequest.teardown_unconfirmed_at.is_(None),
        )
        .values(
            teardown_unconfirmed_at=func.clock_timestamp(),
            # A lease parked at the deadline would keep another worker from
            # claiming the teardown until then.
            runtime_heartbeat_expires_at=_release_parked_hold(),
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    return EnrollmentFence(stopping=stopped, pending_teardown=tuple(pending.all()))


async def record_runtime_termination(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    termination_observation: str,
    expected_work_item_version: int,
    expected_request_version: int,
) -> WorkItemResult:
    return await record_runtime_termination_where(
        session,
        work_item_id=work_item_id,
        request_id=request_id,
        termination_observation=termination_observation,
        expected_work_item_version=expected_work_item_version,
        expected_request_version=expected_request_version,
        extra_where=(),
    )


async def record_runtime_termination_where(
    session: AsyncSession,
    *,
    work_item_id: uuid.UUID,
    request_id: uuid.UUID,
    termination_observation: str,
    expected_work_item_version: int,
    expected_request_version: int,
    extra_where: Sequence[ColumnElement[bool]],
) -> WorkItemResult:
    work_item = await lock_work_item(session, work_item_id)
    if work_item is None:
        return await _conflict(
            session, "not_found", work_item_id=work_item_id, request_id=request_id
        )
    if work_item.version != expected_work_item_version:
        return await _conflict(session, "stale_version", work_item=work_item, request_id=request_id)
    request = await lock_request(session, work_item_id=work_item_id, request_id=request_id)
    if request is None:
        return await _conflict(session, "not_found", work_item=work_item, request_id=request_id)
    if request.version != expected_request_version:
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    if (
        request.status in ("cancelled", *TEARDOWN_TERMINAL_STATUSES)
        and request.terminal_at is not None
        and request.teardown_unconfirmed_at is not None
    ):
        return await _confirm_settled_teardown(
            session,
            work_item=work_item,
            request=request,
            termination_observation=termination_observation,
            extra_where=extra_where,
        )
    if request.status != "cancellation_requested":
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    if not termination_observation.strip():
        return await _conflict(
            session,
            "termination_observation_required",
            work_item=work_item,
            request=request,
        )
    if request.terminal_cause in ("issue_cancelled", "issue_deleted"):
        terminal_status = "cancelled"
    elif request.terminal_cause == "execution_deadline":
        terminal_status = "expired"
    elif request.terminal_cause in ("owner_lost", "enrollment_fenced"):
        terminal_status = "failed"
    else:
        return await _conflict(session, "illegal_transition", work_item=work_item, request=request)
    terminal_cause = request.terminal_cause
    if terminal_status == "failed" and await factory_questions.rounds_exhausted_for(
        session, work_item_id, request_id
    ):
        # Refused past the round cap (#5785, decision 14): losing its owner
        # afterwards does not buy an owner loss successor.
        terminal_cause = factory_questions.QUESTION_ROUNDS_EXHAUSTED
    now = await database_now(session)
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request_id,
            ExecutionRequest.work_item_id == work_item_id,
            ExecutionRequest.version == expected_request_version,
            ExecutionRequest.status == "cancellation_requested",
            *extra_where,
        )
        .values(
            status=terminal_status,
            terminal_cause=terminal_cause,
            terminal_at=now,
            termination_observation=termination_observation,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        request = await reload_request(session, request_id)
        return await _conflict(session, "stale_version", work_item=work_item, request=request)
    request = await reload_request(session, request_id)
    if terminal_status == "expired":
        # The execution deadline ended the session: the status stays expired,
        # and the platform owes its findings (ADR 0277 decision 10).
        await seal_platform_findings(
            session,
            request=request,
            work_item=work_item,
            limit="execution_deadline",
            settings=get_settings(),
        )
    # Before settlement, so the successor keeps the transcript (ADR 0206).
    if await admit_automatic_successor(
        session, work_item, request, origin="owner_loss", disposition=None
    ):
        work_item = await reload_work_item(session, work_item_id)
    await settle_terminal(session, work_item, request, detail=None)
    return await outcome(session, work_item, request)


async def automatic_successor_of(
    session: AsyncSession, request: ExecutionRequest
) -> ExecutionRequest | None:
    """The automatic successor admitted when ``request`` settled, if any (ADR 0269)."""

    row: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest).where(ExecutionRequest.retry_of_request_id == request.id)
    )
    return row


async def admit_automatic_successor(
    session: AsyncSession,
    work_item: WorkItem,
    predecessor: ExecutionRequest,
    *,
    origin: Literal["owner_loss", "ordinary"],
    disposition: Disposition | None,
) -> bool:
    """Admit one fresh waiting request for a failed request (ADR 0206, ADR 0269).

    Call with the WorkItem locked at its current version, after
    ``predecessor`` settled failed, in the same transaction and before
    terminal settlement. The successor re-runs the predecessor's snapshot on
    the WorkItem's recorded base and spends one slot of the WorkItem's shared
    automatic allowance, reserved in the same statement that allocates it.

    Every origin refuses a cancelled WorkItem, a pending relabel, a closed
    pull request lineage, a request its publication still owns, a Curie
    budget refusal, a spent allowance, a tracker result request (it starts
    exactly one model session, ADR 0277 decision 8) and a request whose ask
    was refused for the question round cap (ADR 0286 decision 8), whatever
    its cause, so owner loss cannot outrun the cap. Owner loss keeps its
    teardown based exception: it needs no safety fact and adds no delay. An
    ordinary failure needs the worker's proved ``no_effects``, a policy
    disposition that allows a successor, and no queued newer revision. A
    publication failure instead goes to ``_admit_publication_recovery``.

    A limit condition (#5427) admits a gated successor instead: it carries the
    limit the predecessor's attempt actually had, and the reconciler offers
    it every probe interval until a start delivers strictly more. A
    predecessor without that delivered value admits no successor.

    A successor is admitted blocked only when something can release it: a
    resource gate, or a provider gate whose predecessor names a probed scope
    and no reset time. A condition wait naming any other condition, and a
    successor that would block with no releaser (such as a provider time at
    or past the waiting deadline), admits nothing and spends no slot (#5424).
    """

    if (
        predecessor.status != "failed"
        or predecessor.completion_contract == CONTRACT_TRACKER_RESULT
        or work_item.cancelled_at is not None
        or work_item.readmit_request_id is not None
        or work_item.automatic_retries_used >= AUTOMATIC_RETRY_LIMIT
        or predecessor.terminal_cause
        in ("budget_exceeded", factory_questions.QUESTION_ROUNDS_EXHAUSTED)
        or await factory_questions.rounds_exhausted_for(session, work_item.id, predecessor.id)
    ):
        return False
    if (
        origin == "ordinary"
        and disposition is not None
        and disposition.kind == DispositionKind.PUBLICATION_RECOVERY
    ):
        return await _admit_publication_recovery(session, work_item, predecessor, owner_lost=False)
    if predecessor.execution_kind == "publication_recovery":
        # Another publication failure is admitted above, as publication
        # recovery, before this branch. Owner loss admits the same candidate
        # again inside the shared allowance. Any other failure here admits
        # nothing. None of these paths starts an implementation run (#5429).
        if origin == "owner_loss" and predecessor.terminal_cause == "owner_lost":
            return await _admit_publication_recovery(
                session, work_item, predecessor, owner_lost=True
            )
        return False
    if origin == "owner_loss":
        if predecessor.terminal_cause != "owner_lost":
            return False
    elif (
        predecessor.terminal_cause == "owner_lost"
        or predecessor.retry_safety != "no_effects"
        or disposition is None
        or not disposition.successor
        or (
            disposition.kind == DispositionKind.CONDITION_WAIT
            and disposition.condition not in _RELEASED_CONDITIONS
        )
    ):
        return False
    if await _publication_owns_terminus(
        session, work_item, predecessor
    ) or await _lineage_closed_for(session, work_item, predecessor.reply_target_pr_number):
        return False
    if origin == "ordinary":
        queued = await session.scalar(
            select(ExecutionRequest.id)
            .where(
                ExecutionRequest.work_item_id == work_item.id,
                ExecutionRequest.status == "queued",
            )
            .limit(1)
        )
        if queued is not None:
            return False

    now = await database_now(session)
    wait_deadline = now + timedelta(seconds=get_settings().work_item_wait_budget_seconds)
    slot = work_item.automatic_retries_used + 1
    dispatch_not_before = now
    # Set only for a successor an older dispatcher must never claim.
    guarded_dispatch_not_before: datetime | None = None
    blocked_on: str | None = None
    gate: dict[str, Any] | None = None
    if origin == "ordinary":
        assert disposition is not None
        due = now + timedelta(seconds=automatic_retry_delay_seconds(work_item.id, slot))
        provider_time = disposition.not_before
        if provider_time is not None and provider_time > due:
            due = provider_time
        if disposition.kind == DispositionKind.RETRY_BACKOFF:
            # Without a provider time the successor is always scheduled.
            blocked = provider_time is not None and due >= wait_deadline
        elif disposition.kind == DispositionKind.CONDITION_WAIT:
            # Only a provider time can schedule a condition; otherwise it blocks.
            blocked = provider_time is None or due >= wait_deadline
        else:
            return False
        if disposition.condition in resource_gate.GATE_CONDITIONS:
            gate = resource_gate.gate_for(
                disposition.condition,
                predecessor.delivered_limits,
                source_request_id=predecessor.id,
                source_runtime_epoch=predecessor.runtime_epoch,
            )
            if gate is None:
                return False
            # A gated wait is probed, not slept until its deadline. Only this
            # release reads the gate, so the probe time is guarded: an older
            # dispatcher would start the retry without checking it.
            blocked_on = disposition.condition
            guarded_dispatch_not_before = now + timedelta(
                seconds=get_settings().work_item_resource_gate_probe_seconds
            )
            dispatch_not_before = wait_deadline
        elif blocked:
            # Only a provider probe releases any other block, and only for a
            # probed scope with no reset time; otherwise nothing would wait.
            if not factory_retry_policy.provider_gate_releasable(
                disposition.condition,
                predecessor.retry_condition_reason,
                predecessor.retry_condition_retry_at,
                predecessor.retry_condition_scope_key,
            ):
                return False
            blocked_on = disposition.condition
            dispatch_not_before = wait_deadline
        else:
            dispatch_not_before = due
    admitted = await _insert_waiting_request(
        session,
        work_item,
        request_id=uuid.uuid4(),
        wait_deadline=wait_deadline,
        expected_work_item_version=work_item.version,
        reserve_automatic_retry=True,
        columns={
            "owner_lost_retry": origin == "owner_loss",
            "objective": predecessor.objective,
            "requester": predecessor.requester,
            "reply_kind": predecessor.reply_kind,
            "reply_address": predecessor.reply_address,
            "reply_conversation_id": predecessor.reply_conversation_id,
            "reply_target_kind": predecessor.reply_target_kind,
            "reply_target_pr_number": predecessor.reply_target_pr_number,
            "reply_target_comment_id": predecessor.reply_target_comment_id,
            "reply_target_url": predecessor.reply_target_url,
            # Counted starts span the automatic chain (#5155, ADR 0269).
            "start_deferrals": predecessor.start_deferrals,
            "retry_of_request_id": predecessor.id,
            "retry_origin_request_id": predecessor.retry_origin_request_id or predecessor.id,
            "dispatch_not_before": dispatch_not_before,
            "guarded_dispatch_not_before": guarded_dispatch_not_before,
            "blocked_on": blocked_on,
            "resource_gate": gate,
        },
    )
    # The WorkItem is locked and its count was checked above, so the
    # reservation holds.
    assert admitted
    return True


async def _admit_publication_recovery(
    session: AsyncSession, work_item: WorkItem, predecessor: ExecutionRequest, *, owner_lost: bool
) -> bool:
    """Admit one publication recovery successor for a failed publication (#5429).

    Called from ``admit_automatic_successor`` after its common refusals, in
    the predecessor's settling transaction. The successor reruns no model, so
    only a worker reported ``uncertain`` effect refuses; ``no_effects``,
    ``flagged`` and an API settlement with no worker fact are allowed. The
    predecessor's own settled publication is the failure being recovered, so
    only a publication still in flight on the request, lineage or conversation,
    or a ready applied base move, owns the terminus. A closed lineage, a queued
    newer revision or no exact retained candidate admits nothing.

    The successor is admitted and the candidate handed to it in one savepoint:
    a refused transfer rolls back the request and its allowance slot together.
    """

    from .. import publication_candidates

    if predecessor.retry_safety == "uncertain" or not get_settings().publication_candidates_enabled:
        return False
    scope = Publication.workspace_conversation_id == work_item.conversation_id
    if work_item.publication_lineage_id is not None:
        scope = or_(scope, Publication.lineage_id == work_item.publication_lineage_id)
    in_flight = await session.scalar(
        select(Publication.id)
        .where(
            or_(Publication.execution_request_id == predecessor.id, scope),
            Publication.status.in_(IN_FLIGHT_PUBLICATION),
        )
        .limit(1)
    )
    if (
        in_flight is not None
        or await _applied_base_transition(session, work_item, predecessor, ready=True) is not None
        or await _lineage_closed_for(session, work_item, predecessor.reply_target_pr_number)
    ):
        return False
    queued = await session.scalar(
        select(ExecutionRequest.id)
        .where(ExecutionRequest.work_item_id == work_item.id, ExecutionRequest.status == "queued")
        .limit(1)
    )
    if queued is not None:
        return False
    try:
        candidate = await publication_candidates.recovery_candidate(session, work_item, predecessor)
    except publication_candidates.CandidateRefusal:
        return False
    if candidate is None:
        return False

    now = await database_now(session)
    wait_deadline = now + timedelta(seconds=get_settings().work_item_wait_budget_seconds)
    slot = work_item.automatic_retries_used + 1
    due = now + timedelta(seconds=automatic_retry_delay_seconds(work_item.id, slot))
    request_id = uuid.uuid4()
    try:
        async with session.begin_nested():
            admitted = await _insert_waiting_request(
                session,
                work_item,
                request_id=request_id,
                wait_deadline=wait_deadline,
                expected_work_item_version=work_item.version,
                reserve_automatic_retry=True,
                # The same copied facts as an ordinary automatic successor.
                columns={
                    "owner_lost_retry": owner_lost,
                    "objective": predecessor.objective,
                    "requester": predecessor.requester,
                    "reply_kind": predecessor.reply_kind,
                    "reply_address": predecessor.reply_address,
                    "reply_conversation_id": predecessor.reply_conversation_id,
                    "reply_target_kind": predecessor.reply_target_kind,
                    "reply_target_pr_number": predecessor.reply_target_pr_number,
                    "reply_target_comment_id": predecessor.reply_target_comment_id,
                    "reply_target_url": predecessor.reply_target_url,
                    "start_deferrals": predecessor.start_deferrals,
                    "retry_of_request_id": predecessor.id,
                    "retry_origin_request_id": (
                        predecessor.retry_origin_request_id or predecessor.id
                    ),
                    # An older dispatcher reads no execution kind and would
                    # start this as an implementation run, so only the
                    # guarded time makes it due (migration 0182).
                    "dispatch_not_before": wait_deadline,
                    "guarded_dispatch_not_before": due,
                    "blocked_on": None,
                    "execution_kind": "publication_recovery",
                    "recovery_candidate_id": candidate.id,
                    "recovery_candidate_sha256": candidate.content_sha256,
                },
            )
            # The WorkItem is locked and its count was checked by the caller.
            assert admitted
            await publication_candidates.transfer_ownership(
                session,
                work_item_id=work_item.id,
                candidate_id=candidate.id,
                to_request_id=request_id,
            )
    except publication_candidates.CandidateRefusal:
        # Nothing of the admission stands; reload the rolled back WorkItem.
        await reload_work_item(session, work_item.id)
        return False
    return True


async def _confirm_settled_teardown(
    session: AsyncSession,
    *,
    work_item: WorkItem,
    request: ExecutionRequest,
    termination_observation: str,
    extra_where: Sequence[ColumnElement[bool]],
) -> WorkItemResult:
    """Record a worker teardown a terminal request still owes.

    Either a force-settled cancellation or a started completed, failed or
    expired request an enrollment fence marked (#5377). The request is already
    terminal and its notice already went out, so this only records the
    observation (replacing a settle placeholder) and clears the flag. Its
    status, cause, ``terminal_at`` and runtime epoch stay as they were.
    """

    if not termination_observation.strip():
        return await _conflict(
            session,
            "termination_observation_required",
            work_item=work_item,
            request=request,
        )
    changed_id: uuid.UUID | None = await session.scalar(
        update(ExecutionRequest)
        .where(
            ExecutionRequest.id == request.id,
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.version == request.version,
            ExecutionRequest.status == request.status,
            ExecutionRequest.terminal_at.is_not(None),
            ExecutionRequest.teardown_unconfirmed_at.is_not(None),
            *extra_where,
        )
        .values(
            termination_observation=termination_observation,
            teardown_unconfirmed_at=None,
            runtime_owner=None,
            version=ExecutionRequest.version + 1,
            updated_at=func.clock_timestamp(),
        )
        .returning(ExecutionRequest.id)
    )
    if changed_id is None:
        reloaded = await reload_request(session, request.id)
        return await _conflict(session, "stale_version", work_item=work_item, request=reloaded)
    request = await reload_request(session, request.id)
    return await outcome(session, work_item, request)


_PUBLICATION_CAUSES = {
    "denied": "publication_denied",
    "expired": "publication_expired",
    "failed": "publication_failed",
}


@dataclass(frozen=True)
class PublicationSettlement:
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    work_item_version: int
    request_version: int
    cause: str
    subject: CISubject


async def claim_publication_settlement(
    session: AsyncSession,
    *,
    exclude: frozenset[uuid.UUID],
) -> PublicationSettlement | None:
    """The only reader that decides a linked publication's execution terminus.

    Callers then use ``fail_execution``, or the CI gate for a ``completed``
    settlement (#3097). ``exclude`` skips requests the caller already handled
    this pass, so one request waiting on CI cannot starve the rest. This
    function does not commit and does not write the request.
    """

    rows = (
        await session.execute(
            select(
                ExecutionRequest,
                WorkItem,
                Publication,
                ThreadPublicationLineage.pr_url,
            )
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .join(Publication, Publication.execution_request_id == ExecutionRequest.id)
            .join(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == Publication.lineage_id,
            )
            .where(
                ExecutionRequest.status == "running",
                Publication.status.in_(("denied", "expired", "failed", "succeeded")),
                *((ExecutionRequest.id.not_in(exclude),) if exclude else ()),
            )
            .order_by(ExecutionRequest.updated_at, Publication.revision_number.desc())
            .limit(20)
            .with_for_update(skip_locked=True, of=ExecutionRequest)
        )
    ).all()
    seen: set[uuid.UUID] = set()
    for request, work_item, publication, pr_url in rows:
        if request.id in seen:
            continue
        seen.add(request.id)
        active = await session.scalar(
            select(Publication.id).where(
                Publication.execution_request_id == request.id,
                Publication.status.in_(IN_FLIGHT_PUBLICATION),
            )
        )
        if active is not None:
            continue
        moving = await session.scalar(
            select(FactoryBaseTransition.id)
            .where(
                FactoryBaseTransition.work_item_id == work_item.id,
                or_(
                    FactoryBaseTransition.phase.in_(
                        ("pending", "prepared", "pushed", "retargeted", "ci_prepared", "ci_pushed")
                    ),
                    and_(
                        FactoryBaseTransition.phase == "applied",
                        FactoryBaseTransition.outcome_ready_at.is_(None),
                    ),
                ),
            )
            .limit(1)
        )
        if moving is not None:
            continue
        if publication.status == "succeeded":
            if not isinstance(pr_url, str) or not pr_url.strip():
                continue
            cause = "completed"
        elif publication.status in _PUBLICATION_CAUSES:
            cause = _PUBLICATION_CAUSES[publication.status]
        else:
            continue
        return PublicationSettlement(
            work_item_id=work_item.id,
            request_id=request.id,
            work_item_version=work_item.version,
            request_version=request.version,
            cause=cause,
            subject=PublicationCISubject(
                id=publication.id,
                version=publication.version,
                head_sha=await session.scalar(
                    select(ThreadPublicationLineage.head_sha).where(
                        ThreadPublicationLineage.id == publication.lineage_id
                    )
                )
                or "",
                base_ref=await session.scalar(
                    select(ThreadPublicationLineage.base_ref).where(
                        ThreadPublicationLineage.id == publication.lineage_id
                    )
                ),
                changed_paths=tuple(publication.changed_paths),
                published_at=publication.terminal_at or publication.updated_at,
            ),
        )
    moves = (
        await session.execute(
            select(ExecutionRequest, WorkItem, FactoryBaseTransition)
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .join(FactoryBaseTransition, FactoryBaseTransition.ci_request_id == ExecutionRequest.id)
            .where(
                ExecutionRequest.status == "running",
                ready_base_transition_exists(),
                *((ExecutionRequest.id.not_in(exclude),) if exclude else ()),
            )
            .order_by(ExecutionRequest.updated_at)
            .limit(20)
            .with_for_update(skip_locked=True, of=ExecutionRequest)
        )
    ).all()
    for request, item, move in moves:
        if move.verification_head_sha is None:
            continue
        return PublicationSettlement(
            work_item_id=item.id,
            request_id=request.id,
            work_item_version=item.version,
            request_version=request.version,
            cause="completed",
            subject=BaseTransitionCISubject(
                id=move.id,
                version=move.version,
                head_sha=move.verification_head_sha,
                base_ref=move.target_base_ref,
                changed_paths=tuple(move.changed_paths),
                published_at=move.ci_pushed_at or move.applied_at,
            ),
        )
    return None
