"""Database access for lineages."""

import re
import uuid
from typing import Any

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from curie_api.schemas.publications import PublicationLineageAdvance, ReviewRevisionReserve

from ..config import get_settings
from ..forges.errors import Unavailable
from ..forges.types import GITHUB
from ..models import (
    AgentChannel,
    Deployment,
    ExecutionRequest,
    FactoryBaseTransition,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadWorkspace,
    WorkItem,
)
from ..publication_authority import CodeHostPublicationIdentity, PublicationIdentity
from ..publication_verification import VerifiedPublicationCommit, stored_publication_base_ref
from ..repository_access import thread_repository
from ..threadkeys import route_thread_key_matches, thread_key_forms
from ..workspace_policy import repository_allowed
from .deployments import get_deployment
from .errors import PublicationLineageConflict
from .workspaces import get_thread_workspace

# The lineage columns that hold its verified code host identity, in the order
# a `PublicationIdentity`'s `lineage_values` returns them.
_IDENTITY_COLUMNS = (
    "code_host_kind",
    "code_host_host",
    "repository_project_id",
    "code_host_installation_id",
    "code_host_pr_id",
    "base_ref",
)


def lineage_identity(lineage: ThreadPublicationLineage) -> tuple[Any, ...]:
    return tuple(getattr(lineage, column) for column in _IDENTITY_COLUMNS)


async def _bind_running_work_item_lineage(
    session: AsyncSession,
    *,
    publication: Publication,
    lineage: ThreadPublicationLineage,
) -> None:
    """Record the publishing request's open PR even after execution ends."""

    if (
        publication.execution_request_id is None
        or lineage.status != "open"
        or lineage.pr_number is None
    ):
        return
    request_owns_item = (
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.id == publication.execution_request_id,
            ExecutionRequest.work_item_id == WorkItem.id,
        )
        .exists()
    )
    other_owner = aliased(WorkItem)
    predicates: list[ColumnElement[bool]] = [
        WorkItem.agent_id == lineage.agent_id,
        WorkItem.conversation_id.in_(
            await thread_key_forms(session, lineage.agent_id, lineage.conversation_id)
        ),
        func.lower(WorkItem.repository_path) == lineage.repo_full_name.casefold(),
        WorkItem.cancelled_at.is_(None),
        WorkItem.publication_lineage_id.is_(None),
        request_owns_item,
        ~select(other_owner.id).where(other_owner.publication_lineage_id == lineage.id).exists(),
    ]
    predicates.extend(
        (
            WorkItem.code_host_kind == lineage.code_host_kind,
            WorkItem.code_host_host == lineage.code_host_host,
            WorkItem.repository_project_id == lineage.repository_project_id,
            WorkItem.code_host_installation_id.is_not_distinct_from(
                lineage.code_host_installation_id
            ),
        )
    )
    await session.execute(
        update(WorkItem)
        .where(*predicates)
        .values(
            publication_lineage_id=lineage.id,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
    )


async def select_thread_publication_lineage(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
    repo_full_name: str,
    for_update: bool = False,
) -> ThreadPublicationLineage | None:
    statement = (
        select(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.agent_id == agent_id,
            ThreadPublicationLineage.conversation_id == conversation_id,
            ThreadPublicationLineage.repo_full_name == repo_full_name,
        )
        .order_by(ThreadPublicationLineage.created_at.desc())
        .limit(1)
    )
    if for_update:
        statement = statement.with_for_update().execution_options(populate_existing=True)
    lineage: ThreadPublicationLineage | None = await session.scalar(statement)
    return lineage


async def get_thread_publication_lineage(
    session: AsyncSession,
    *,
    deployment_id: uuid.UUID,
    conversation_id: str,
    repo_full_name: str,
) -> ThreadPublicationLineage | None:
    """Read one authorized thread lineage without exposing credentials."""

    deployment = await get_deployment(session, deployment_id)
    if deployment is None:
        raise LookupError("deployment not found")
    selected = await get_thread_workspace(
        session,
        agent_id=deployment.agent_id,
        conversation_id=conversation_id,
    )
    if selected is None:
        raise ValueError("conversation has no selected repository workspace")
    if selected.repo_full_name.casefold() != repo_full_name.casefold():
        raise ValueError("publication repository differs from the thread workspace")
    settings = get_settings()
    repository = await thread_repository(
        session,
        settings,
        agent_id=deployment.agent_id,
        conversation_id=conversation_id,
        repo_full_name=selected.repo_full_name,
    )
    if not repository_allowed(settings, repository):
        raise ValueError("thread workspace repository is no longer allowed")
    return await select_thread_publication_lineage(
        session,
        agent_id=deployment.agent_id,
        conversation_id=conversation_id,
        repo_full_name=selected.repo_full_name,
    )


async def publication_lineage_has_pending_revision(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return whether an active publication revision owns this lineage."""

    if lineage.status != "open":
        return False
    pending_id = await session.scalar(
        select(Publication.id)
        .where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("pending", "approved", "launching", "running")),
        )
        .limit(1)
    )
    moving = await session.scalar(
        select(FactoryBaseTransition.id)
        .where(
            FactoryBaseTransition.lineage_id == lineage.id,
            or_(
                FactoryBaseTransition.phase.in_(
                    ("pending", "prepared", "pushed", "retargeted", "ci_prepared", "ci_pushed")
                ),
                and_(
                    FactoryBaseTransition.phase.in_(("conflict", "refused")),
                    FactoryBaseTransition.request_id.in_(
                        select(ExecutionRequest.id).where(
                            ExecutionRequest.status.in_(
                                ("waiting", "running", "cancellation_requested")
                            )
                        )
                    ),
                ),
            ),
        )
        .limit(1)
    )
    return pending_id is not None or moving is not None


async def _publication_lineage_has_reserved_review(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return whether a verified review currently reserves this lineage."""

    if lineage.status != "open":
        return False
    return (
        await session.scalar(
            select(PublicationReviewReservation.id)
            .where(
                PublicationReviewReservation.lineage_id == lineage.id,
                PublicationReviewReservation.status == "reserved",
            )
            .limit(1)
        )
        is not None
    )


async def publication_lineage_has_pending_outcome(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return whether a terminal result still owes durable thread history."""

    pending_id = await session.scalar(
        select(Publication.id)
        .where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("denied", "expired", "succeeded", "failed")),
            Publication.outcome_history_ready_at.is_(None),
        )
        .limit(1)
    )
    moving = await session.scalar(
        select(FactoryBaseTransition.id)
        .where(
            FactoryBaseTransition.lineage_id == lineage.id,
            FactoryBaseTransition.outcome_ready_at.is_(None),
            FactoryBaseTransition.phase == "applied",
        )
        .limit(1)
    )
    return pending_id is not None or moving is not None


async def publication_lineage_visible_outcome_revision(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> int:
    """Return the newest revision already present in durable thread history."""

    revision = await session.scalar(
        select(func.max(Publication.revision_number)).where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("denied", "expired", "succeeded", "failed")),
            Publication.outcome_history_ready_at.is_not(None),
        )
    )
    return int(revision or 0)


async def publication_lineage_has_inflight_push(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return whether an authorized revision may currently be changing GitHub."""

    if lineage.status != "open":
        return False
    inflight_id = await session.scalar(
        select(Publication.id)
        .where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("approved", "launching", "running")),
        )
        .limit(1)
    )
    return inflight_id is not None


async def initialize_publication_lineage_head(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
    *,
    expected_version: int,
    head_sha: str,
) -> ThreadPublicationLineage:
    """CAS-initialize the unknown head of a migrated URL-only lineage."""

    changed = await session.execute(
        update(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.id == lineage.id,
            ThreadPublicationLineage.status == "open",
            ThreadPublicationLineage.version == expected_version,
            ThreadPublicationLineage.head_sha.is_(None),
            ThreadPublicationLineage.pr_number == lineage.pr_number,
            ThreadPublicationLineage.pr_url == lineage.pr_url,
        )
        .values(
            head_sha=head_sha,
            version=ThreadPublicationLineage.version + 1,
            updated_at=func.now(),
        )
        .returning(ThreadPublicationLineage.id)
    )
    if changed.scalar_one_or_none() is None:
        await session.rollback()
        current = await session.get(ThreadPublicationLineage, lineage.id)
        if (
            current is not None
            and current.status == "open"
            and current.head_sha == head_sha
            and current.pr_number == lineage.pr_number
            and current.pr_url == lineage.pr_url
        ):
            return current
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage changed while its migrated head was initialized",
        )
    await session.commit()
    refreshed = await session.get(ThreadPublicationLineage, lineage.id)
    assert refreshed is not None
    await session.refresh(refreshed)
    return refreshed


async def mark_publication_lineage_terminal(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
    *,
    expected_version: int,
    expected_head_sha: str,
    state: str,
) -> ThreadPublicationLineage:
    """Persist observed terminal GitHub truth without changing the known head."""

    if state not in ("merged", "closed"):
        raise ValueError("terminal publication lineage state is invalid")
    changed = await session.execute(
        update(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.id == lineage.id,
            ThreadPublicationLineage.status == "open",
            ThreadPublicationLineage.version == expected_version,
            ThreadPublicationLineage.head_sha == expected_head_sha,
            ThreadPublicationLineage.pr_number == lineage.pr_number,
            ThreadPublicationLineage.pr_url == lineage.pr_url,
        )
        .values(
            status=state,
            version=ThreadPublicationLineage.version + 1,
            updated_at=func.now(),
        )
        .returning(ThreadPublicationLineage.id)
    )
    if changed.scalar_one_or_none() is None:
        await session.rollback()
        current = await session.get(ThreadPublicationLineage, lineage.id)
        if (
            current is not None
            and current.status == state
            and current.head_sha == expected_head_sha
            and current.pr_number == lineage.pr_number
            and current.pr_url == lineage.pr_url
        ):
            return current
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage changed while its terminal state was refreshed",
        )
    await session.commit()
    refreshed = await session.get(ThreadPublicationLineage, lineage.id)
    assert refreshed is not None
    await session.refresh(refreshed)
    return refreshed


def publication_lineage_outcome_conflict(
    publication: Publication,
    lineage: ThreadPublicationLineage,
    data: PublicationLineageAdvance,
    *,
    pull_request_url: str,
) -> PublicationLineageConflict | None:
    """Preconditions one revision outcome must meet before it may claim a lineage.

    Pure, so the route can reject a stale outcome before it contacts GitHub and
    the advancing writer can repeat the same verdict under its row locks. The
    order is load bearing and carried over unchanged. Which check fires first
    decides which conflict code the caller sees, and the worker branches on it.
    ``pull_request_url`` is the canonical link of the outcome's pull request
    (`outcome_pull_request_url`).
    """

    if lineage.status != "open":
        return PublicationLineageConflict(
            "publication.lineage_terminal",
            "the pull request for this thread is merged or closed; start a new thread",
        )
    if publication.revision_number != lineage.latest_revision:
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "publication revision is not the current thread lineage revision",
        )
    # Case insensitive on both sides: `_validated_pr_url` in the worker accepts
    # GitHub's own spelling of the repository and preserves it, so a repository
    # whose GitHub casing differs from `repo_full_name` publishes fine and must
    # not then take a stable refusal here.
    if data.pr_url.casefold() != pull_request_url.casefold():
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request identity does not match the publication repository",
        )
    if lineage.pr_number is not None and (
        lineage.pr_number != data.pr_number
        or (lineage.pr_url or "").casefold() != data.pr_url.casefold()
    ):
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request identity no longer matches the stored thread lineage",
        )
    if lineage.version != data.expected_version or lineage.head_sha != data.expected_head_sha:
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage version or expected head is stale",
        )
    if (
        publication.version != data.expected_publication_version
        or publication.lease_owner != data.lease_owner
    ):
        return PublicationLineageConflict(
            "publication.lease_lost",
            "publication lease is no longer held by this worker",
        )
    if publication.status not in ("approved", "launching", "running"):
        return PublicationLineageConflict(
            "publication.revision_not_approved",
            "publication revision must be approved before advancing its lineage",
        )
    needs_metadata_timestamp = data.state == "open" and not publication.patch_bytes
    if needs_metadata_timestamp != (data.metadata_updated_at is not None):
        return PublicationLineageConflict(
            "publication.metadata_timestamp_invalid",
            "a GitHub update time is required only for metadata only success",
        )
    return None


def outcome_pull_request_url(
    lineage: ThreadPublicationLineage,
    data: PublicationLineageAdvance,
    *,
    github_html_base: str,
    identity: PublicationIdentity | None,
) -> str:
    """The canonical link of an outcome's pull request.

    A code host verified through its port (`CodeHostPublicationIdentity`)
    names its pull request itself; a GitHub lineage's is under the install's
    HTML base.
    """

    if isinstance(identity, CodeHostPublicationIdentity):
        return identity.pr_url
    return f"{github_html_base}/{lineage.repo_full_name}/pull/{data.pr_number}"


async def advance_publication_lineage(
    session: AsyncSession,
    publication_id: uuid.UUID,
    data: PublicationLineageAdvance,
    *,
    github_html_base: str,
    first_parent_proof: VerifiedPublicationCommit | None,
    identity: PublicationIdentity | None = None,
) -> ThreadPublicationLineage:
    """Atomically advance one approved revision and its exact lineage head."""

    publication = await session.scalar(
        select(Publication)
        .where(Publication.id == publication_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if publication is None:
        raise LookupError("publication not found")
    if publication.lineage_id is None:
        raise PublicationLineageConflict(
            "publication.lineage_absent",
            "publication has no thread pull request lineage",
        )
    execution_request: ExecutionRequest | None = None
    work_item: WorkItem | None = None
    if publication.execution_request_id is not None:
        result = await session.execute(
            select(WorkItem, ExecutionRequest)
            .join(ExecutionRequest, ExecutionRequest.work_item_id == WorkItem.id)
            .where(ExecutionRequest.id == publication.execution_request_id)
            .with_for_update(of=(WorkItem, ExecutionRequest))
            .execution_options(populate_existing=True)
        )
        row = result.one_or_none()
        if row is not None:
            work_item, execution_request = row
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == publication.lineage_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if lineage is None:
        raise PublicationLineageConflict(
            "publication.lineage_absent",
            "publication thread pull request lineage is absent",
        )
    conflict = publication_lineage_outcome_conflict(
        publication,
        lineage,
        data,
        pull_request_url=outcome_pull_request_url(
            lineage, data, github_html_base=github_html_base, identity=identity
        ),
    )
    if conflict is not None:
        raise conflict

    if (lineage.head_sha is None) != (lineage.pr_number is None):
        raise PublicationLineageConflict(
            "publication.lineage_stale", "stored publication lineage is incomplete"
        )
    first_publication = lineage.head_sha is None and lineage.pr_number is None
    if first_publication and first_parent_proof is None:
        raise PublicationLineageConflict(
            "publication.lineage_stale", "first publication requires a verified commit proof"
        )
    base_values: dict[str, str] = {}
    if first_parent_proof is not None:
        proof = first_parent_proof
        if (
            not isinstance(proof, VerifiedPublicationCommit)
            or proof.publication_id != publication.id
            or proof.publication_version != publication.version
            or proof.lineage_id != lineage.id
            or proof.lineage_version != lineage.version
            or proof.approved_base_sha != publication.base_sha
            or proof.expected_prior_head != publication.expected_prior_head
            or proof.commit_sha != data.head_sha
            or proof.first_publication != first_publication
            or re.fullmatch(r"[0-9a-f]{40,64}", proof.parent_sha) is None
            or proof.parent_sha == proof.commit_sha
            or (identity is not None and identity.base_ref != proof.base_branch)
        ):
            raise PublicationLineageConflict(
                "publication.lineage_stale", "verified publication commit proof is stale"
            )
        try:
            base_branch = stored_publication_base_ref(
                publication, lineage, execution_request, work_item
            )
        except Unavailable:
            raise PublicationLineageConflict(
                "publication.lineage_stale", "the persisted publication base is unproved"
            ) from None
        if base_branch is not None and base_branch != proof.base_branch:
            raise PublicationLineageConflict(
                "publication.lineage_stale", "the authoritative publication base branch changed"
            )
        if first_publication:
            if (
                lineage.base_sha != publication.base_sha
                or publication.expected_prior_head != publication.base_sha
                or publication.patch_bytes == b""
            ):
                raise PublicationLineageConflict(
                    "publication.lineage_stale", "the approved first publication base changed"
                )
            base_values["base_sha"] = proof.parent_sha
        elif proof.parent_sha != publication.expected_prior_head:
            raise PublicationLineageConflict(
                "publication.lineage_stale", "verified revision has the wrong expected parent"
            )

    identity_values: dict[str, Any] = {}
    if identity is not None:
        verified = identity.lineage_values()
        if lineage.repository_project_id is None:
            if lineage.pr_number is not None or lineage.binding_id is None:
                raise PublicationLineageConflict(
                    "publication.review_ineligible",
                    "historical lineage identity cannot be reconstructed",
                )
        elif lineage_identity(lineage) != tuple(verified[column] for column in _IDENTITY_COLUMNS):
            raise PublicationLineageConflict(
                "publication.lineage_stale",
                "immutable GitHub lineage identity changed",
            )
        await require_current_lineage_workspace(
            session,
            lineage,
            conflict_code="publication.lineage_stale",
            conflict_message="publication workspace or deployment is no longer authorized",
        )
        identity_values = verified
    elif lineage.repository_project_id is not None:
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "verified lineage advance requires current GitHub identity",
        )

    head_predicate = (
        ThreadPublicationLineage.head_sha.is_(None)
        if data.expected_head_sha is None
        else ThreadPublicationLineage.head_sha == data.expected_head_sha
    )
    changed = await session.execute(
        update(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.id == lineage.id,
            ThreadPublicationLineage.status == "open",
            ThreadPublicationLineage.version == data.expected_version,
            head_predicate,
        )
        .values(
            **identity_values,
            **base_values,
            pr_number=data.pr_number,
            pr_url=data.pr_url,
            head_sha=data.head_sha,
            status=data.state,
            version=ThreadPublicationLineage.version + 1,
            updated_at=func.now(),
        )
        .returning(ThreadPublicationLineage.id)
    )
    if changed.scalar_one_or_none() is None:
        await session.rollback()
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage changed before this revision could advance it",
        )

    terminal_state = data.state in ("merged", "closed")
    publication_status = "failed" if terminal_state else "succeeded"
    publication_values: dict[str, Any] = {
        "status": publication_status,
        "version": Publication.version + 1,
        "patch_bytes": None,
        # Settle the worker's publication lease with the outcome, exactly as
        # its terminal CAS would, so the result outbox is claimable at once.
        "lease_owner": None,
        "lease_expires_at": None,
        "terminal_at": func.now(),
        "updated_at": func.now(),
        "result_url": data.pr_url,
        "metadata_updated_at": data.metadata_updated_at,
        # Success replaces an earlier attempt's error, as the worker CAS did.
        "error": None,
    }
    if terminal_state:
        publication_values["error"] = (
            "the pull request for this thread is merged or closed; start a new thread"
        )
    settled = await session.execute(
        update(Publication)
        .where(
            Publication.id == publication.id,
            Publication.status == publication.status,
            Publication.version == data.expected_publication_version,
            Publication.lease_owner == data.lease_owner,
        )
        .values(**publication_values)
        .returning(Publication.id)
    )
    if settled.scalar_one_or_none() is None:
        await session.rollback()
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "publication revision changed before its lineage could advance",
        )
    if not terminal_state:
        await _bind_running_work_item_lineage(
            session,
            publication=publication,
            lineage=lineage,
        )
    await session.commit()
    refreshed = await session.get(ThreadPublicationLineage, lineage.id)
    assert refreshed is not None
    await session.refresh(refreshed)
    return refreshed


async def require_current_lineage_workspace(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
    *,
    conflict_code: str,
    conflict_message: str,
) -> None:
    """Recheck the workspace and deployment that still authorize publication."""

    workspace = await session.scalar(
        select(ThreadWorkspace)
        .where(
            ThreadWorkspace.agent_id == lineage.agent_id,
            ThreadWorkspace.conversation_id == lineage.conversation_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    deployment = await session.get(
        Deployment,
        lineage.deployment_id,
        with_for_update=True,
        populate_existing=True,
    )
    settings = get_settings()
    repository = lineage.repository or await thread_repository(
        session,
        settings,
        agent_id=lineage.agent_id,
        conversation_id=lineage.conversation_id,
        repo_full_name=lineage.repo_full_name,
    )
    if (
        workspace is None
        or workspace.repo_full_name.casefold() != lineage.repo_full_name.casefold()
        or not repository_allowed(settings, repository)
        or deployment is None
        or deployment.agent_id != lineage.agent_id
        or deployment.status != "active"
    ):
        raise PublicationLineageConflict(
            conflict_code,
            conflict_message,
        )


async def require_review_binding(
    session: AsyncSession, lineage: ThreadPublicationLineage
) -> AgentChannel:
    """Recheck original authority under the same transaction as reservation use."""

    binding = (
        await session.get(
            AgentChannel,
            lineage.binding_id,
            with_for_update=True,
            populate_existing=True,
        )
        if lineage.binding_id
        else None
    )
    await require_current_lineage_workspace(
        session,
        lineage,
        conflict_code="publication.review_ineligible",
        conflict_message="original review binding or workspace is no longer authorized",
    )
    if (
        binding is None
        or binding.agent_id != lineage.agent_id
        or binding.generation != lineage.binding_generation
        or not lineage.reply_conversation_id
        or not route_thread_key_matches(
            binding.kind,
            binding.adapter,
            binding.address,
            lineage.reply_conversation_id,
            lineage.conversation_id,
        )
    ):
        raise PublicationLineageConflict(
            "publication.review_ineligible",
            "original review binding or workspace is no longer authorized",
        )
    return binding


async def reserve_review_revision(
    session: AsyncSession,
    data: ReviewRevisionReserve,
) -> tuple[PublicationReviewReservation, ThreadPublicationLineage, bool]:
    """Reserve in the caller's transaction, so feedback insertion can be atomic.

    No commit, approval, queue entry, or GitHub write occurs here. A reservation
    is consumed only by PublicationCreate naming its exact accepted origin.
    """

    from ..forges.hosts import github_host

    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.code_host_kind == GITHUB,
            ThreadPublicationLineage.code_host_host == github_host(get_settings()),
            ThreadPublicationLineage.repository_project_id == str(data.repository_id),
            ThreadPublicationLineage.pr_number == data.pr_number,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        lineage is None
        or lineage.status != "open"
        or lineage.head_sha is None
        or lineage.code_host_installation_id is None
        or lineage.code_host_pr_id is None
        or lineage.base_ref is None
    ):
        raise PublicationLineageConflict(
            "publication.review_ineligible",
            "no verified open lineage owns this GitHub pull request",
        )
    binding = await require_review_binding(session, lineage)
    existing = await session.scalar(
        select(PublicationReviewReservation)
        .where(PublicationReviewReservation.origin_key == data.origin_key)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if existing is not None:
        if (
            existing.lineage_id != lineage.id
            or existing.lineage_version != data.expected_lineage_version
        ):
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "review origin was replayed with different lineage facts",
            )
        return existing, lineage, False
    if lineage.version != data.expected_lineage_version:
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "review expected a stale lineage version",
        )
    if (
        await publication_lineage_has_pending_revision(session, lineage)
        or await _publication_lineage_has_reserved_review(session, lineage)
        or await publication_lineage_has_pending_outcome(session, lineage)
    ):
        raise PublicationLineageConflict(
            "publication.revision_conflict",
            "a revision or its durable outcome already owns this lineage",
        )
    row = PublicationReviewReservation(
        id=uuid.uuid4(),
        origin_key=data.origin_key,
        lineage_id=lineage.id,
        lineage_version=lineage.version,
        expected_head_sha=lineage.head_sha,
        revision_number=lineage.latest_revision + 1,
        binding_id=binding.id,
        binding_generation=binding.generation,
        status="reserved",
        version=1,
    )
    session.add(row)
    try:
        await session.flush()
    except IntegrityError:
        # Caller owns rollback, including its feedback insertion. A reused origin
        # on a different PR cannot be adopted by this transaction.
        raise PublicationLineageConflict(
            "publication.revision_conflict",
            "review origin is already reserved",
        ) from None
    return row, lineage, True


async def cancel_review_revision(
    session: AsyncSession,
    reservation_id: uuid.UUID,
    *,
    origin_key: str,
    expected_version: int,
) -> PublicationReviewReservation:
    row = await session.scalar(
        select(PublicationReviewReservation)
        .where(PublicationReviewReservation.id == reservation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise LookupError("review reservation not found")
    if row.origin_key != origin_key or row.version != expected_version or row.status != "reserved":
        raise PublicationLineageConflict(
            "publication.revision_conflict",
            "review reservation changed before cancellation",
        )
    row.status = "cancelled"
    row.version += 1
    row.updated_at = func.now()
    await session.flush()
    return row
