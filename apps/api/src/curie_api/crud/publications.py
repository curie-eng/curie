"""Database access for publications."""

import hashlib
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from aci_protocol.turn import route_identity
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.channels import BUILTIN_CLUSTER_MESSAGE_ADAPTER
from curie_api.schemas.publications import PublicationCreate

from ..config import get_settings
from ..models import (
    Approval,
    ApprovalAuditEntry,
    ApprovalStatus,
    CredentialRedemptionAuditEntry,
    Deployment,
    ExecutionRequest,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadWorkspace,
    WorkItem,
)
from ..publication_policy import (
    PLATFORM_ACTOR,
    PLATFORM_AUTHORIZER,
    POLICY_AUTO,
    POLICY_IDENTITY,
    publication_branch_name,
    publication_row_prefix,
)
from ..threadkeys import fence_key_forms, legacy_producer_thread_key
from ..workspace_policy import repository_is_allowed
from .agents import get_agent
from .approvals import get_approval_by_dedupe_key
from .channels import AmbiguousRoute, binding_for_route
from .deployments import get_deployment
from .errors import PublicationLineageConflict, PublicationReplayConflict
from .lineages import require_review_binding, select_thread_publication_lineage
from .publication_queries import get_publication_by_approval
from .workspaces import get_thread_workspace


async def _adopt_publication_replay(
    session: AsyncSession,
    data: PublicationCreate,
    patch: bytes,
) -> Publication | None:
    """Adopt an exact replay only through its persisted authorization lane."""

    approval = await get_approval_by_dedupe_key(session, data.dedupe_key)
    if approval is None:
        return None
    publication = await get_publication_by_approval(session, approval.id)
    if publication is None:
        raise PublicationReplayConflict(
            "publication dedupe key belongs to a non-publication approval"
        )
    deployment = await get_deployment(session, data.deployment_id)
    if deployment is None:
        raise LookupError("deployment not found")
    review_origin = await session.scalar(
        select(PublicationReviewReservation.origin_key).where(
            PublicationReviewReservation.id == publication.id
        )
    )
    if review_origin != data.review_origin_key:
        raise PublicationReplayConflict("publication replay has a different review origin")
    reply_conversation_id = data.reply_conversation_id or data.conversation_id
    workspace_conversation_id = (
        data.conversation_id
        if data.reply_conversation_id is not None
        else legacy_producer_thread_key(
            data.reply_kind,
            data.reply_channel,
            data.conversation_id,
        )
    )
    if (
        approval.agent_id != deployment.agent_id
        or approval.conversation_id != reply_conversation_id
        or approval.reply_kind != data.reply_kind
        or approval.reply_channel != data.reply_channel
        or approval.reply_placeholder != data.reply_placeholder
        or approval.reply_endpoint != data.reply_endpoint
        or route_identity(approval.reply_kind, approval.reply_adapter)
        != route_identity(data.reply_kind, data.reply_adapter)
        or publication.deployment_id != data.deployment_id
        or publication.repo_full_name.casefold() != data.repo_full_name.casefold()
        or publication.base_sha != data.base_sha
        or publication.patch_bytes != patch
        or publication.changed_paths != data.changed_paths
        or publication.observed_title_sha256
        != (
            hashlib.sha256(data.observed_title.encode()).hexdigest()
            if data.observed_title is not None
            else None
        )
        or publication.observed_body_sha256 != data.observed_body_sha256
        or publication.title != (data.title or data.summary)
        or publication.body != (data.body or "Approved platform publication.")
        or publication.reply_kind != data.reply_kind
        or publication.reply_channel != data.reply_channel
        or publication.reply_placeholder != data.reply_placeholder
        or publication.reply_endpoint != data.reply_endpoint
        or route_identity(publication.reply_kind, publication.reply_adapter)
        != route_identity(data.reply_kind, data.reply_adapter)
        or publication.lineage is None
        or publication.lineage.agent_id != deployment.agent_id
        or publication.lineage.conversation_id != workspace_conversation_id
        or publication.lineage.repo_full_name.casefold() != publication.repo_full_name.casefold()
    ):
        raise PublicationReplayConflict(
            "publication dedupe key was replayed with different snapshot facts"
        )
    if publication.workspace_conversation_id is None:
        # 0041 canonicalizes every genuinely legacy row. A NULL observed after
        # that migration is corruption or an artificial downgrade lane, never
        # authority to fall back to an adapter-native reply id.
        raise PublicationReplayConflict("publication replay has no canonical workspace identity")
    authorized_conversation_id = publication.workspace_conversation_id
    await _require_current_publication_workspace(
        session,
        data,
        conversation_id=authorized_conversation_id,
        deployment=deployment,
    )
    return publication


async def _require_current_publication_workspace(
    session: AsyncSession,
    data: PublicationCreate,
    *,
    conversation_id: str,
    deployment: Deployment | None = None,
) -> tuple[Deployment, ThreadWorkspace]:
    """Authorize the request against current deployment and thread policy."""

    if deployment is None:
        deployment = await get_deployment(session, data.deployment_id)
    if deployment is None:
        raise LookupError("deployment not found")
    thread_workspace = await get_thread_workspace(
        session,
        agent_id=deployment.agent_id,
        conversation_id=conversation_id,
    )
    if thread_workspace is None:
        raise ValueError("conversation has no selected repository workspace")
    if thread_workspace.repo_full_name.casefold() != data.repo_full_name.casefold():
        raise ValueError("publication repository differs from the thread workspace")
    if not repository_is_allowed(
        thread_workspace.repo_full_name, get_settings().github_repo_allowlist
    ):
        raise ValueError("thread workspace repository is no longer allowed")
    return deployment, thread_workspace


# -- approvals (#244, ADR-0010) -------------------------------------------------


_ACTIVE_WORK_ITEM_STATUSES = ("waiting", "running", "cancellation_requested")


async def _work_item_for_thread(
    session: AsyncSession, *, agent_id: uuid.UUID, conversation_id: str
) -> WorkItem | None:
    """This agent's work item on this thread, under its key or its pre-identity
    key (ADR-0168 decision 4).

    Unguarded (`fence_key_forms`, not `thread_key_forms`): both callers are a
    REFUSAL already scoped to `agent_id`, where over-matching is the safe
    direction, and the guard would fail open the moment the binding that
    proved the old key is gone -- exactly when a cancelled legacy work item
    still has to fence credential redemption.
    """

    for form in fence_key_forms(conversation_id):
        work_item: WorkItem | None = await session.scalar(
            select(WorkItem)
            .where(WorkItem.agent_id == agent_id, WorkItem.conversation_id == form)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        if work_item is not None:
            return work_item
    return None


async def publication_cancellation_conflict(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
) -> PublicationLineageConflict | None:
    """Refuse credential redemption after the owning work item is cancelled.

    A running request may still publish. Cancellation of the work item, or an
    active request already in ``cancellation_requested``, may not.
    """

    work_item = await _work_item_for_thread(
        session, agent_id=agent_id, conversation_id=conversation_id
    )
    if work_item is None:
        return None
    if work_item.cancelled_at is not None:
        return PublicationLineageConflict(
            "publication.work_item_cancelled",
            "this conversation's work item is cancelled",
        )
    active = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.status == "cancellation_requested",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if active is None:
        return None
    return PublicationLineageConflict(
        "publication.work_item_cancelled",
        "this conversation's work item is cancelled",
    )


async def _refuse_fenced_work_item(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
    request_id: uuid.UUID | None,
    runtime_epoch: int | None,
) -> ExecutionRequest | None:
    work_item = await _work_item_for_thread(
        session, agent_id=agent_id, conversation_id=conversation_id
    )
    if work_item is None:
        return None
    if work_item.cancelled_at is not None:
        raise PublicationLineageConflict(
            "publication.work_item_cancelled",
            "this conversation's work item is cancelled",
        )
    active = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.status.in_(_ACTIVE_WORK_ITEM_STATUSES),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if active is None:
        return None
    if active.status == "cancellation_requested":
        raise PublicationLineageConflict(
            "publication.work_item_cancelled",
            "this conversation's work item is cancelled",
        )
    # A resumed approval turn does not carry the execute event id. The only
    # running request for this conversation owns the publication.
    if request_id is None and runtime_epoch is None:
        return active
    if active.status == "running" and (
        request_id != active.id or runtime_epoch != active.runtime_epoch
    ):
        raise PublicationLineageConflict(
            "publication.work_item_stale_owner",
            "the publication is not owned by the running work item request",
        )
    return active


async def create_publication(
    session: AsyncSession,
    data: PublicationCreate,
    *,
    patch: bytes,
    metadata_check: Callable[[], Awaitable[None]],
    traceparent: str | None = None,
) -> tuple[Publication, bool]:
    """Atomically create the durable approval and its private publication.

    ``dedupe_key`` belongs to Approval, so the replay lookup starts there. An
    exact replay adopts both rows; a changed patch or snapshot fact is a hard
    conflict and can never replace bytes that were already approved.
    """

    existing = await _adopt_publication_replay(session, data, patch)
    if existing is not None:
        await session.refresh(existing, ["lineage"])
        return existing, False
    await metadata_check()
    if bool(patch) != bool(data.changed_paths):
        raise PublicationLineageConflict(
            "publication.invalid_snapshot",
            "publication patch and changed paths must both be present or both be empty",
        )

    workspace_conversation_id = (
        data.conversation_id
        if data.reply_conversation_id is not None
        else legacy_producer_thread_key(
            data.reply_kind,
            data.reply_channel,
            data.conversation_id,
        )
    )
    deployment, thread_workspace = await _require_current_publication_workspace(
        session,
        data,
        conversation_id=workspace_conversation_id,
    )
    agent = await get_agent(session, deployment.agent_id)
    if agent is None:
        raise LookupError("agent not found")
    auto = agent.publication_policy == POLICY_AUTO

    owned_request = await _refuse_fenced_work_item(
        session,
        agent_id=deployment.agent_id,
        conversation_id=workspace_conversation_id,
        request_id=data.work_item_request_id,
        runtime_epoch=data.work_item_runtime_epoch,
    )
    lineage = await select_thread_publication_lineage(
        session,
        agent_id=deployment.agent_id,
        conversation_id=workspace_conversation_id,
        repo_full_name=thread_workspace.repo_full_name,
        for_update=True,
    )
    if not patch and (lineage is None or lineage.pr_number is None):
        raise PublicationLineageConflict(
            "publication.metadata_requires_pull",
            "a metadata-only revision requires an existing pull request",
        )
    reservation: PublicationReviewReservation | None = None
    if lineage is None:
        if data.review_origin_key is not None:
            raise PublicationLineageConflict(
                "publication.review_ineligible",
                "review origin has no existing lineage",
            )
        # The route this publication replies through, scoped to its agent: one
        # agent can hold one channel under several identities (ADR-0168
        # decision 3), and the lineage must capture the one it was raised
        # under, never whichever row sorted first.
        try:
            binding = await binding_for_route(
                session,
                data.reply_kind,
                data.reply_adapter,
                data.reply_channel,
                agent_id=deployment.agent_id,
                for_update=True,
            )
        except AmbiguousRoute:
            binding = None
        if binding is not None and not _binding_route_matches(
            data, data.reply_kind, binding.endpoint, binding.adapter
        ):
            binding = None
        lineage_id = uuid.uuid4()
        lineage = ThreadPublicationLineage(
            id=lineage_id,
            agent_id=deployment.agent_id,
            deployment_id=deployment.id,
            conversation_id=workspace_conversation_id,
            repo_full_name=thread_workspace.repo_full_name,
            base_sha=data.base_sha,
            branch=publication_branch_name(
                lineage_id.hex,
                prefix=agent.publication_branch_prefix,
                auto=auto,
            ),
            status="open",
            version=1,
            latest_revision=1,
            binding_id=binding.id if binding is not None else None,
            binding_generation=binding.generation if binding is not None else None,
            reply_conversation_id=data.reply_conversation_id or data.conversation_id,
        )
        session.add(lineage)
        try:
            await session.flush()
        except IntegrityError as exc:
            # A distinct first request can race this INSERT. Its approval and
            # publication have not been flushed, so the losing transaction can
            # safely roll back without leaving a second human decision.
            await session.rollback()
            existing = await _adopt_publication_replay(session, data, patch)
            if existing is not None:
                await session.refresh(existing, ["lineage"])
                return existing, False
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "another publication revision already owns this thread lineage",
            ) from exc
        revision_number = 1
    else:
        if lineage.status != "open":
            raise PublicationLineageConflict(
                "publication.lineage_terminal",
                "the pull request for this thread is merged or closed; start a new thread",
            )
        pending_outcome = await session.scalar(
            select(Publication.id).where(
                Publication.lineage_id == lineage.id,
                Publication.status.in_(("denied", "expired", "succeeded", "failed")),
                Publication.outcome_history_ready_at.is_(None),
            )
        )
        if pending_outcome is not None:
            raise PublicationLineageConflict(
                "publication.outcome_pending",
                "the previous publication outcome is not yet durable in thread history",
            )
        expected_prior_head = lineage.head_sha or lineage.base_sha
        if data.base_sha != expected_prior_head:
            raise PublicationLineageConflict(
                "publication.lineage_stale",
                "the managed checkout is not at the current pull request head",
            )
        in_flight = await session.scalar(
            select(Publication.id).where(
                Publication.lineage_id == lineage.id,
                Publication.status.in_(("pending", "approved", "launching", "running")),
            )
        )
        if in_flight is not None:
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "another publication revision is still in progress for this thread",
            )
        reservation = await session.scalar(
            select(PublicationReviewReservation)
            .where(
                PublicationReviewReservation.lineage_id == lineage.id,
                PublicationReviewReservation.status == "reserved",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if reservation is not None:
            if (
                data.review_origin_key != reservation.origin_key
                or reservation.lineage_version != lineage.version
                or reservation.expected_head_sha != expected_prior_head
            ):
                raise PublicationLineageConflict(
                    "publication.revision_conflict",
                    "a different review origin or stale head owns this revision",
                )
            review_binding = await require_review_binding(session, lineage)
            if (
                data.reply_kind != review_binding.kind
                or data.reply_channel != review_binding.address
                or not _binding_route_matches(
                    data, review_binding.kind, review_binding.endpoint, review_binding.adapter
                )
                or (data.reply_conversation_id or data.conversation_id)
                != lineage.reply_conversation_id
            ):
                raise PublicationLineageConflict(
                    "publication.review_ineligible",
                    "review publication reply differs from its reserved original binding",
                )
            revision_number = reservation.revision_number
            reservation.status = "consumed"
            reservation.version += 1
            reservation.updated_at = func.now()
        elif data.review_origin_key is not None:
            raise PublicationLineageConflict(
                "publication.review_ineligible",
                "review origin has no active reservation",
            )
        else:
            revision_number = lineage.latest_revision + 1
        lineage.latest_revision = revision_number
        lineage.updated_at = func.now()

    if (
        auto
        and agent.publication_branch_prefix
        and not lineage.branch.startswith(agent.publication_branch_prefix)
    ):
        raise PublicationLineageConflict(
            "publication.branch_prefix",
            "the publication branch does not carry the operator branch prefix",
        )

    expected_prior_head = lineage.head_sha or lineage.base_sha
    expires_at = None
    if data.expires_in_seconds is not None:
        expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(
            seconds=data.expires_in_seconds
        )
    resolved_at = datetime.now(UTC).replace(tzinfo=None) if auto else None
    approval = Approval(
        id=uuid.uuid4(),
        agent_id=deployment.agent_id,
        conversation_id=data.reply_conversation_id or data.conversation_id,
        author=data.author,
        summary=data.summary,
        reply_kind=data.reply_kind,
        reply_channel=data.reply_channel,
        reply_placeholder=data.reply_placeholder,
        reply_endpoint=data.reply_endpoint,
        reply_adapter=data.reply_adapter,
        dedupe_key=data.dedupe_key,
        traceparent=traceparent,
        route=data.route,
        card_channel=data.reply_channel,
        gate_kind="permission",
        granted_tool="mcp__curie__publish_changes",
        purpose="publication",
        expires_at=expires_at,
        status=ApprovalStatus.approved if auto else ApprovalStatus.pending,
        resolved_by=PLATFORM_ACTOR if auto else None,
        resolution_note=(
            f"authorized by {POLICY_IDENTITY} version {agent.publication_policy_version}"
            if auto
            else None
        ),
        resolved_at=resolved_at,
        resumed_at=resolved_at,
        policy_identity=POLICY_IDENTITY if auto else None,
        policy_version=agent.publication_policy_version if auto else None,
    )
    session.add(approval)
    publication = Publication(
        id=reservation.id if reservation is not None else uuid.uuid4(),
        approval=approval,
        deployment_id=deployment.id,
        workspace_conversation_id=workspace_conversation_id,
        lineage=lineage,
        revision_number=revision_number,
        expected_prior_head=expected_prior_head,
        repo_full_name=thread_workspace.repo_full_name,
        status="approved" if auto else "pending",
        open_as_draft=bool(auto and agent.publication_draft),
        branch_prefix=publication_row_prefix(
            lineage.branch,
            operator_prefix=agent.publication_branch_prefix,
            auto=auto,
        ),
        approval_card_reported_at=resolved_at,
        version=1,
        base_sha=data.base_sha,
        patch_bytes=patch,
        changed_paths=data.changed_paths,
        observed_title_sha256=(
            hashlib.sha256(data.observed_title.encode()).hexdigest()
            if data.observed_title is not None
            else None
        ),
        observed_body_sha256=data.observed_body_sha256,
        title=data.title or data.summary,
        body=data.body or "Approved platform publication.",
        reply_kind=data.reply_kind,
        reply_channel=data.reply_channel,
        reply_placeholder=data.reply_placeholder,
        reply_endpoint=data.reply_endpoint,
        reply_adapter=data.reply_adapter,
    )
    if owned_request is not None:
        publication.execution_request_id = owned_request.id
    session.add(publication)
    if auto:
        session.add(
            ApprovalAuditEntry(
                approval_id=approval.id,
                action="resolved",
                actor=PLATFORM_ACTOR,
                actor_channel=None,
                principal_kind="platform",
                authenticated=True,
                decision=ApprovalStatus.approved,
                authorizer=PLATFORM_AUTHORIZER,
                authorized=True,
                reason=approval.resolution_note,
                evidence={
                    "policy_identity": POLICY_IDENTITY,
                    "policy_version": agent.publication_policy_version,
                    "agent_id": str(agent.id),
                    "publication_draft": bool(agent.publication_draft),
                    "publication_branch_prefix": agent.publication_branch_prefix,
                },
            )
        )
    try:
        await session.commit()
    except IntegrityError as exc:
        # The dedupe key and active-revision indexes arbitrate deliveries that
        # raced after the locked lineage read. Adopt only an exact replay.
        await session.rollback()
        existing = await _adopt_publication_replay(session, data, patch)
        if existing is None:
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "another publication revision already owns this thread lineage",
            ) from exc
        await session.refresh(existing, ["lineage"])
        return existing, False
    await session.refresh(publication)
    await session.refresh(publication, ["lineage"])
    return publication, True


def _binding_route_matches(
    data: PublicationCreate, kind: str, endpoint: str | None, adapter: str | None
) -> bool:
    """Whether a stored binding route is the one a publication's reply names.

    The built-in cluster-message relay is not a configurable route: the channel
    API reserves its adapter, so the binding it replies for is one with no
    route of its own (#2789) -- no endpoint, and the identity an omitted
    adapter resolves to, which on Slack is 'default' (ADR-0168 decision 3).
    Any configured route is compared through `route_identity`, not the raw
    column, so a handle that names no identity and a stored `'default'` match.
    """

    if data.reply_adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER:
        return endpoint is None and route_identity(kind, adapter) == route_identity(kind, None)
    return endpoint == data.reply_endpoint and route_identity(kind, adapter) == route_identity(
        data.reply_kind, data.reply_adapter
    )


async def append_credential_redemption_audit(
    session: AsyncSession,
    *,
    purpose: str,
    outcome: str,
    deployment_id: uuid.UUID | None,
    publication_id: uuid.UUID | None,
    repo_full_name: str | None,
    detail: str | None,
) -> None:
    session.add(
        CredentialRedemptionAuditEntry(
            purpose=purpose,
            outcome=outcome,
            deployment_id=deployment_id,
            publication_id=publication_id,
            repo_full_name=repo_full_name,
            detail=detail,
        )
    )
    await session.commit()


async def reap_terminal_publication_patches(
    session: AsyncSession, *, terminal_before: datetime, limit: int
) -> int:
    ids = list(
        await session.scalars(
            select(Publication.id)
            .where(
                Publication.status.in_(("denied", "expired", "succeeded", "failed")),
                Publication.terminal_at.is_not(None),
                Publication.terminal_at <= terminal_before,
                Publication.patch_bytes.is_not(None),
            )
            .order_by(Publication.terminal_at)
            .limit(limit)
        )
    )
    if not ids:
        return 0
    await session.execute(
        update(Publication)
        .where(Publication.id.in_(ids))
        .values(patch_bytes=None, updated_at=func.now())
    )
    await session.commit()
    return len(ids)
