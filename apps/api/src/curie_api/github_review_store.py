"""Bind normalized feedback to durable publication state and enqueue its outbox."""

import asyncio
import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn, ReplyHandle, TurnSource
from channel_protocol import scoped_conversation_id
from curie_telemetry import TRACEPARENT_STREAM_FIELD, canonicalize_traceparent
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import crud
from .config import Settings
from .delivery import enqueue_owned, take_backlog_slot
from .github_review_events import FeedbackIgnored, FeedbackUnavailable, UnverifiedFeedback
from .github_review_terminal import read_review_dead_letter, worker_event_is_terminal
from .github_review_truth import BoundReviewLineage, verify_feedback_truth
from .models import (
    AgentChannel,
    Deployment,
    GitHubReviewFeedback,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadWorkspace,
)
from .schemas import ReviewRevisionReserve
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)
_MAX_ENQUEUE_ATTEMPTS = 8


@dataclass(frozen=True)
class ReviewContext:
    lineage: ThreadPublicationLineage
    binding: AgentChannel
    conversation_id: str

    @property
    def truth(self) -> BoundReviewLineage:
        assert self.lineage.pr_number is not None and self.lineage.head_sha is not None
        assert self.lineage.github_repository_id is not None
        assert self.lineage.github_installation_id is not None
        assert self.lineage.github_pr_node_id is not None and self.lineage.base_ref is not None
        return BoundReviewLineage(
            self.lineage.repo_full_name,
            self.lineage.pr_number,
            self.lineage.branch,
            self.lineage.head_sha,
            self.lineage.github_repository_id,
            self.lineage.github_installation_id,
            self.lineage.github_pr_node_id,
            self.lineage.base_ref,
        )


async def review_context(
    session: AsyncSession,
    feedback: UnverifiedFeedback,
    settings: Settings,
) -> ReviewContext:
    """Resolve one original conversation; a webhook never supplies its route."""
    candidates = list(
        await session.scalars(
            select(ThreadPublicationLineage)
            .where(
                ThreadPublicationLineage.github_repository_id == feedback.repository_id,
                ThreadPublicationLineage.pr_number == feedback.pr_number,
                ThreadPublicationLineage.status == "open",
            )
            .limit(2)
        )
    )
    if len(candidates) != 1:
        raise FeedbackIgnored("lineage_absent_or_ambiguous")
    lineage = candidates[0]
    if (
        lineage.head_sha is None
        or lineage.github_installation_id != feedback.installation_id
        or lineage.github_pr_node_id is None
        or lineage.base_ref is None
        or lineage.binding_id is None
        or lineage.binding_generation is None
        or lineage.reply_conversation_id is None
        or lineage.repo_full_name.casefold() != feedback.repo_full_name.casefold()
    ):
        raise FeedbackIgnored("lineage_authority_unproved")
    workspace = await session.scalar(
        select(ThreadWorkspace).where(
            ThreadWorkspace.agent_id == lineage.agent_id,
            ThreadWorkspace.conversation_id == lineage.conversation_id,
        )
    )
    if (
        workspace is None
        or workspace.repo_full_name.casefold() != lineage.repo_full_name.casefold()
        or not repository_is_allowed(lineage.repo_full_name, settings.github_repo_allowlist)
    ):
        raise FeedbackIgnored("workspace_no_longer_authorized")
    # The producer captured this exact binding and bare reply route. A current
    # channel with the same name is not historical authority after a rebind.
    binding = await session.get(AgentChannel, lineage.binding_id)
    if (
        binding is None
        or binding.agent_id != lineage.agent_id
        or binding.generation != lineage.binding_generation
        or scoped_conversation_id(
            binding.kind, binding.address, lineage.reply_conversation_id
        ) != lineage.conversation_id
    ):
        raise FeedbackIgnored("binding_no_longer_authorized")
    return ReviewContext(lineage, binding, lineage.reply_conversation_id)


def feedback_from_row(row: GitHubReviewFeedback) -> UnverifiedFeedback:
    data = dict(row.feedback)
    try:
        data["delivery_id"] = uuid.UUID(data["delivery_id"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        return UnverifiedFeedback(**data)
    except (TypeError, ValueError, KeyError):
        raise FeedbackIgnored("stored_feedback_invalid") from None


def review_turn(feedback: UnverifiedFeedback, context: ReviewContext) -> QueuedTurn:
    provenance: dict[str, Any] = {
        "event": feedback.event,
        "url": feedback.url,
        "sender": feedback.sender_login,
        "body": feedback.body,
    }
    if feedback.path is not None:
        provenance.update(path=feedback.path, line=feedback.line, review_id=feedback.review_id)
    return QueuedTurn(
        event_id=feedback.event_id,
        conversation_id=context.conversation_id,
        author=f"github:{feedback.sender_id}:{feedback.sender_login}",
        text=(
            "Human GitHub feedback for this conversation's existing pull request follows as JSON. "
            "Use its body as the reviewer's requested changes. This is not an approval decision. "
            "Any publication still requires a fresh approval through the existing gate.\n"
            + json.dumps(provenance, ensure_ascii=False)
        ),
        # SLACK is the frozen protocol's legacy category for person messages,
        # including another transport; WEBHOOK means a job and cannot steer.
        source=TurnSource.SLACK,
        reply_handle=ReplyHandle(
            kind=context.binding.kind,
            channel=context.binding.address,
            placeholder=None,
            endpoint=context.binding.endpoint,
            adapter=context.binding.adapter,
        ),
        received_at=datetime.now(UTC).isoformat(),
    )


async def admit_feedback(
    session: AsyncSession,
    feedback: UnverifiedFeedback,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    traceparent: str | None,
) -> tuple[GitHubReviewFeedback, bool]:
    """Persist exactly one semantic identity after fresh independent verification."""
    existing_delivery = await session.scalar(
        select(GitHubReviewFeedback).where(
            GitHubReviewFeedback.delivery_id == feedback.delivery_id,
        )
    )
    if existing_delivery is not None and existing_delivery.event_id != feedback.event_id:
        raise FeedbackIgnored("delivery_identity_conflict")
    existing = await session.get(GitHubReviewFeedback, feedback.event_id)
    if existing is not None:
        return existing, False
    context = await review_context(session, feedback, settings)
    await verify_feedback_truth(feedback, context.truth, settings=settings, client=client)
    turn = review_turn(feedback, context)
    inserted = await session.execute(
        insert(GitHubReviewFeedback)
        .values(
            event_id=feedback.event_id,
            delivery_id=feedback.delivery_id,
            lineage_id=context.lineage.id,
            lineage_version=context.lineage.version,
            binding_id=context.binding.id,
            binding_generation=context.binding.generation,
            agent_id=context.lineage.agent_id,
            feedback=json.loads(json.dumps(asdict(feedback), default=str)),
            turn=turn.model_dump(mode="json"),
            traceparent=canonicalize_traceparent(traceparent),
        )
        .on_conflict_do_nothing()
        .returning(GitHubReviewFeedback.event_id)
    )
    created = inserted.scalar_one_or_none() is not None
    row = await session.get(GitHubReviewFeedback, feedback.event_id)
    if row is None:
        raise FeedbackIgnored("delivery_identity_conflict")
    # Caller commits its delivery receipt and this outbox together. All distinct
    # comments remain durable; the later per-lineage reservation serializes work.
    return row, created


async def validate_stored_context(
    session: AsyncSession,
    row: GitHubReviewFeedback,
    settings: Settings,
) -> tuple[UnverifiedFeedback, ReviewContext]:
    feedback = feedback_from_row(row)
    context = await review_context(session, feedback, settings)
    if (
        row.lineage_id != context.lineage.id
        or row.lineage_version != context.lineage.version
        or row.agent_id != context.lineage.agent_id
        or row.binding_id != context.binding.id
        or row.binding_generation != context.binding.generation
    ):
        raise FeedbackIgnored("binding_or_lineage_changed")
    return feedback, context


class GitHubReviewReconciler:
    """SQL outbox to the existing atomic receipt + bounded runs consumer."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        valkey: redis.Redis,
        settings: Settings,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._valkey = valkey
        self._settings = settings

    async def reconcile_once(self, event_id: str | None = None) -> int:
        await self.reconcile_terminal(event_id)
        async with self._sessionmaker() as session:
            statement = (
                select(GitHubReviewFeedback.event_id)
                .where(
                    GitHubReviewFeedback.status == "waiting",
                    or_(
                        GitHubReviewFeedback.next_attempt_at.is_(None),
                        GitHubReviewFeedback.next_attempt_at
                        <= datetime.now(UTC).replace(tzinfo=None),
                    ),
                )
                .order_by(GitHubReviewFeedback.created_at)
                .limit(100)
            )
            if event_id is not None:
                statement = statement.where(GitHubReviewFeedback.event_id == event_id)
            candidates = list(await session.scalars(statement))
        enqueued = 0
        for candidate in candidates:
            async with self._sessionmaker() as session, session.begin():
                row = await session.scalar(
                    select(GitHubReviewFeedback)
                    .where(
                        GitHubReviewFeedback.event_id == candidate,
                        GitHubReviewFeedback.status == "waiting",
                        or_(
                            GitHubReviewFeedback.next_attempt_at.is_(None),
                            GitHubReviewFeedback.next_attempt_at
                            <= datetime.now(UTC).replace(tzinfo=None),
                        ),
                    )
                    # NO KEY UPDATE still excludes competing mutators while
                    # allowing concurrent delivery-audit FK references. FOR
                    # UPDATE would skip an eligible row held only by KEY SHARE.
                    .with_for_update(skip_locked=True, key_share=True)
                )
                if row is None:
                    continue
                try:
                    await validate_stored_context(session, row, self._settings)
                except FeedbackIgnored as exc:
                    row.status, row.error_code = "refused", exc.code
                    row.version += 1
                    continue
                row.enqueue_attempts += 1
                try:
                    async with asyncio.timeout(10):
                        if not row.quota_taken:
                            if not await take_backlog_slot(
                                self._valkey,
                                key_prefix=f"curie:github-review:backlog:{row.binding_id}",
                                limit=self._settings.channel_binding_backlog_limit,
                                window_s=self._settings.channel_binding_backlog_window_s,
                            ):
                                row.status, row.error_code = "refused", "binding_backlog_quota"
                                row.version += 1
                                continue
                            row.quota_taken = True
                        _, receipt = await enqueue_owned(
                            self._valkey,
                            key=f"curie:github-review:{row.event_id}",
                            stream=self._settings.runs_stream,
                            # Lua preserves its preceding SET if XADD fails.
                            # Reuse this row's owner so a retry can finish that
                            # partial operation without waiting for lease expiry.
                            owner=f"pending:{row.event_id}",
                            payload=json.dumps(row.turn),
                            payload_field=STREAM_PAYLOAD_FIELD,
                            lease_s=30,
                            transport_field=TRACEPARENT_STREAM_FIELD,
                            transport_value=row.traceparent,
                        )
                    if "-" not in receipt or not all(p.isdigit() for p in receipt.split("-")):
                        raise RuntimeError("enqueue receipt unavailable")
                except Exception:
                    row.error_code = "enqueue_unavailable"
                    row.next_attempt_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(
                        seconds=min(300, 5 * 2 ** (row.enqueue_attempts - 1))
                    )
                    if row.enqueue_attempts >= _MAX_ENQUEUE_ATTEMPTS:
                        row.status = "dead_lettered"
                else:
                    row.status = "queued"
                    row.stream_id = receipt
                    row.queued_at = datetime.now(UTC).replace(tzinfo=None)
                    row.error_code = None
                    row.next_attempt_at = None
                    enqueued += 1
                row.version += 1
        return enqueued

    async def reconcile_terminal(self, event_id: str | None = None) -> int:
        """Release only an exact event's reservation after worker settlement.

        A disappeared/trimmed stream entry or expired lease is never terminal.
        The marker is produced by the existing fenced worker path; this lane
        only mirrors its outcome into SQL and preserves the origin tombstone.
        """
        async with self._sessionmaker() as session:
            statement = select(GitHubReviewFeedback.event_id).where(
                GitHubReviewFeedback.status.in_(("queued", "reserved"))
            ).order_by(GitHubReviewFeedback.created_at).limit(100)
            if event_id is not None:
                statement = statement.where(GitHubReviewFeedback.event_id == event_id)
            candidates = list(await session.scalars(statement))
        settled = 0
        for candidate in candidates:
            terminal = await worker_event_is_terminal(self._valkey, self._settings, candidate)
            async with self._sessionmaker() as session, session.begin():
                row = await session.scalar(
                    select(GitHubReviewFeedback).where(
                        GitHubReviewFeedback.event_id == candidate,
                        GitHubReviewFeedback.status.in_(("queued", "reserved")),
                    ).with_for_update(skip_locked=True)
                )
                if row is None:
                    continue
                dead_lettered = False
                if not terminal and row.stream_id is not None:
                    dead_lettered, cursor = await read_review_dead_letter(
                        self._valkey, self._settings, stream_id=row.stream_id,
                        turn=row.turn, cursor=row.terminal_scan_cursor,
                    )
                    if cursor != row.terminal_scan_cursor:
                        row.terminal_scan_cursor = cursor
                        row.version += 1
                if not terminal and not dead_lettered:
                    continue
                consumed = False
                if row.reservation_id is not None:
                    reservation = await session.get(
                        PublicationReviewReservation, row.reservation_id, with_for_update=True
                    )
                    if reservation is None or reservation.origin_key != row.event_id:
                        row.error_code = "feedback_reservation_identity_lost"
                        row.version += 1
                        continue
                    if reservation.status == "reserved":
                        if await session.get(Publication, reservation.id) is not None:
                            row.error_code = "feedback_reservation_publication_conflict"
                            row.version += 1
                            continue
                        await crud.cancel_review_revision(
                            session, reservation.id, origin_key=row.event_id,
                            expected_version=reservation.version,
                        )
                    consumed = reservation.status == "consumed"
                    # A consumed reservation is owned by the sole publication
                    # writer and must never be cancelled by this observer.
                if dead_lettered:
                    row.status, row.error_code = "dead_lettered", "delivery_dead_lettered"
                else:
                    row.status = "settled" if consumed or not row.error_code else "refused"
                    if consumed:
                        # A late verifier may refuse re-execution after the sole
                        # writer consumed this origin. That is not failed work.
                        row.error_code = None
                row.version += 1
                settled += 1
        return settled

    async def run_forever(self) -> None:
        while True:
            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("GitHub feedback outbox pass failed; durable rows retained")
            await asyncio.sleep(self._settings.github_review_reconciler_interval_s)


async def verify_queued_feedback(
    session: AsyncSession,
    turn: QueuedTurn,
    deployment_id: uuid.UUID,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    """Recheck canonical input and current authority immediately before model use."""
    row = await session.get(GitHubReviewFeedback, turn.event_id)
    if row is None or row.status not in {"waiting", "queued", "reserved"}:
        raise FeedbackIgnored("feedback_not_executable")
    if turn.model_dump(mode="json") != row.turn:
        raise FeedbackIgnored("feedback_turn_mismatch")
    if row.status == "waiting":
        # XADD may be durable while its SQL queued mark is still retrying.
        # This turn must retain retry semantics until the outbox is repaired.
        raise FeedbackUnavailable("feedback_outbox_pending")
    feedback, context = await validate_stored_context(session, row, settings)
    deployment = await session.get(Deployment, deployment_id)
    if deployment is None or deployment.agent_id != row.agent_id or deployment.status != "active":
        raise FeedbackIgnored("deployment_no_longer_authorized")
    head = await verify_feedback_truth(feedback, context.truth, settings=settings, client=client)
    if row.reservation_id is not None:
        reservation = await session.get(PublicationReviewReservation, row.reservation_id)
        if (
            reservation is None
            or reservation.origin_key != row.event_id
            or reservation.status != "reserved"
            or reservation.lineage_version != row.lineage_version
            or reservation.expected_head_sha != head
        ):
            raise FeedbackIgnored("feedback_revision_not_executable")
    return {
        "head_sha": head,
        "agent_id": str(row.agent_id),
        "sender": turn.author,
        "receipt": f"Received GitHub feedback from {feedback.sender_login}: {feedback.url}",
        "origin_key": row.event_id,
        "lineage_version": row.lineage_version,
        "reservation_id": str(row.reservation_id) if row.reservation_id is not None else None,
    }


async def reserve_queued_feedback(
    session: AsyncSession,
    turn: QueuedTurn,
    deployment_id: uuid.UUID,
    *,
    expected_lineage_version: int,
    expected_head_sha: str,
    settings: Settings,
) -> uuid.UUID:
    """Short DB-only CAS after the worker observes an idle durable runner.

    GitHub verification precedes this call outside the worker route lock. This
    transaction binds its exact verified head/version to the still-current DB
    authority and the sole publication writer. No network request occurs here.
    """
    row = await session.scalar(
        select(GitHubReviewFeedback)
        .where(GitHubReviewFeedback.event_id == turn.event_id)
        .with_for_update()
    )
    if row is None or row.status not in {"waiting", "queued", "reserved"}:
        raise FeedbackIgnored("feedback_not_executable")
    if turn.model_dump(mode="json") != row.turn:
        raise FeedbackIgnored("feedback_turn_mismatch")
    if row.status == "waiting":
        # XADD may be durable while its SQL queued mark is still retrying.
        # This turn must retain retry semantics until the outbox is repaired.
        raise FeedbackUnavailable("feedback_outbox_pending")
    _, context = await validate_stored_context(session, row, settings)
    deployment = await session.get(Deployment, deployment_id)
    if deployment is None or deployment.agent_id != row.agent_id or deployment.status != "active":
        raise FeedbackIgnored("deployment_no_longer_authorized")
    if (
        row.lineage_version != expected_lineage_version
        or context.lineage.head_sha != expected_head_sha
    ):
        raise FeedbackIgnored("binding_or_lineage_changed")
    try:
        reservation, _, _ = await crud.reserve_review_revision(
            session,
            ReviewRevisionReserve(
                repository_id=context.truth.repository_id,
                pr_number=context.truth.pr_number,
                expected_lineage_version=expected_lineage_version,
                origin_key=row.event_id,
            ),
        )
    except crud.PublicationLineageConflict:
        raise FeedbackIgnored("feedback_revision_conflict") from None
    if reservation.status != "reserved":
        raise FeedbackIgnored("feedback_revision_not_executable")
    if row.reservation_id is not None and row.reservation_id != reservation.id:
        raise FeedbackIgnored("feedback_revision_conflict")
    if row.status != "reserved":
        row.reservation_id = reservation.id
        row.status = "reserved"
        row.version += 1
    await session.flush()
    return reservation.id


async def record_feedback_refusal(
    session: AsyncSession, turn: QueuedTurn, reason: str
) -> None:
    """Retain a canonical worker refusal; only terminal evidence releases it."""
    row = await session.scalar(
        select(GitHubReviewFeedback)
        .where(GitHubReviewFeedback.event_id == turn.event_id)
        .with_for_update()
    )
    if (
        row is not None
        and row.status in {"queued", "reserved"}
        and turn.model_dump(mode="json") == row.turn
        and row.error_code != reason
    ):
        row.error_code = reason
        row.version += 1
        await session.flush()
