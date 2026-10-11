"""Database access for lineages."""

import uuid
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from curie_api.schemas.publications import PublicationLineageAdvance, ReviewRevisionReserve

from ..config import get_settings
from ..forges.types import GITHUB
from ..models import (
    AgentChannel,
    Deployment,
    ExecutionRequest,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadWorkspace,
    WorkItem,
)
from ..publication_authority import VerifiedPublicationIdentity
from ..threadkeys import route_thread_key_matches, thread_key_forms
from ..workspace_policy import repository_is_allowed
from .deployments import get_deployment
from .errors import PublicationLineageConflict
from .workspaces import get_thread_workspace

# The lineage columns that hold its verified code host identity, in the order
# `VerifiedPublicationIdentity.lineage_values` returns them.
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
    identity: VerifiedPublicationIdentity | None,
) -> None:
    """Bind only the running request that created the successful publication."""

    if publication.execution_request_id is None:
        return
    request_owns_item = (
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.id == publication.execution_request_id,
            ExecutionRequest.work_item_id == WorkItem.id,
            ExecutionRequest.status == "running",
        )
        .exists()
    )
    predicates: list[ColumnElement[bool]] = [
        WorkItem.agent_id == lineage.agent_id,
        WorkItem.conversation_id.in_(
            await thread_key_forms(session, lineage.agent_id, lineage.conversation_id)
        ),
        func.lower(WorkItem.repository_path) == lineage.repo_full_name.casefold(),
        WorkItem.cancelled_at.is_(None),
        WorkItem.publication_lineage_id.is_(None),
        request_owns_item,
    ]
    values = identity.lineage_values() if identity is not None else None
    kind, host, project_id, installation_id = (
        (
            values["code_host_kind"],
            values["code_host_host"],
            values["repository_project_id"],
            values["code_host_installation_id"],
        )
        if values is not None
        else (
            lineage.code_host_kind,
            lineage.code_host_host,
            lineage.repository_project_id,
            lineage.code_host_installation_id,
        )
    )
    if project_id is not None:
        predicates.extend(
            (
                WorkItem.code_host_kind == kind,
                WorkItem.code_host_host == host,
                WorkItem.repository_project_id == project_id,
            )
        )
    if installation_id is not None:
        predicates.append(WorkItem.code_host_installation_id == installation_id)
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
    if not repository_is_allowed(repo_full_name, get_settings().github_repo_allowlist):
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
    return pending_id is not None


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
    return pending_id is not None


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
    github_html_base: str,
) -> PublicationLineageConflict | None:
    """Preconditions one revision outcome must meet before it may claim a lineage.

    Pure, so the route can reject a stale outcome before it contacts GitHub and
    the advancing writer can repeat the same verdict under its row locks. The
    order is load bearing and carried over unchanged. Which check fires first
    decides which conflict code the caller sees, and the worker branches on it.
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
    canonical = f"{github_html_base}/{lineage.repo_full_name}/pull/{data.pr_number}"
    if data.pr_url.casefold() != canonical.casefold():
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


async def advance_publication_lineage(
    session: AsyncSession,
    publication_id: uuid.UUID,
    data: PublicationLineageAdvance,
    *,
    github_html_base: str,
    identity: VerifiedPublicationIdentity | None = None,
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
    if publication.execution_request_id is not None:
        await session.scalar(
            select(WorkItem.id)
            .join(ExecutionRequest, ExecutionRequest.work_item_id == WorkItem.id)
            .where(ExecutionRequest.id == publication.execution_request_id)
            .with_for_update(of=WorkItem)
        )
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
        publication, lineage, data, github_html_base=github_html_base
    )
    if conflict is not None:
        raise conflict

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
            identity=identity,
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
    if (
        workspace is None
        or workspace.repo_full_name.casefold() != lineage.repo_full_name.casefold()
        or not repository_is_allowed(lineage.repo_full_name, get_settings().github_repo_allowlist)
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
