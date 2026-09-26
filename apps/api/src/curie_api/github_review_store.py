"""Bind normalized feedback to durable publication state and enqueue its outbox."""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import redis.asyncio as redis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn, ReplyHandle, TurnSource
from channel_protocol import scoped_conversation_id
from curie_telemetry import TRACEPARENT_STREAM_FIELD, canonicalize_traceparent
from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import crud
from .config import Settings
from .delivery import enqueue_owned, take_backlog_slot
from .github_review_audit import settle_review_delivery
from .github_review_events import (
    FeedbackHeld,
    FeedbackIgnored,
    FeedbackUnavailable,
    UnverifiedFeedback,
)
from .github_review_terminal import read_review_dead_letter, worker_event_is_terminal
from .github_review_truth import BoundReviewLineage, verify_feedback_truth
from .models import (
    AgentChannel,
    Deployment,
    GitHubReviewDelivery,
    GitHubReviewFeedback,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadWorkspace,
)
from .schemas import BUILTIN_CLUSTER_MESSAGE_ADAPTER, ReviewRevisionReserve
from .workspace_policy import repository_is_allowed

logger = logging.getLogger(__name__)
_MAX_ENQUEUE_ATTEMPTS = 8


@dataclass(frozen=True)
class ReviewContext:
    lineage: ThreadPublicationLineage
    binding: AgentChannel
    conversation_id: str
    # The reply adapter of the publication that opened this lineage. The
    # built-in cluster-message relay binds a route-less channel (#2789), so the
    # binding alone would send the review reply to the Slack sink.
    origin_adapter: str | None = None

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
    if not candidates and await _identity_pending(session, feedback):
        raise FeedbackHeld()
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
    origin_adapter = await session.scalar(
        select(Publication.reply_adapter)
        .where(Publication.lineage_id == lineage.id)
        .order_by(Publication.revision_number)
        .limit(1)
    )
    return ReviewContext(lineage, binding, lineage.reply_conversation_id, origin_adapter)


async def _identity_pending(session: AsyncSession, feedback: UnverifiedFeedback) -> bool:
    """Whether a new bound lineage for this repository still awaits GitHub identity.

    Publication creates the lineage before the PR exists and records identity
    only after the worker observes the PR (#2962). Feedback in that gap must be
    held, not refused. No age bound applies here: publication may take any
    time before the PR exists. The hold's own lifetime (held_at plus the hold
    window) bounds how long an unrelated PR in the same repository waits
    before its refusal.
    """
    pending = await session.scalar(
        select(ThreadPublicationLineage.id)
        .where(
            ThreadPublicationLineage.status == "open",
            ThreadPublicationLineage.github_repository_id.is_(None),
            ThreadPublicationLineage.pr_number.is_(None),
            ThreadPublicationLineage.binding_id.is_not(None),
            func.lower(ThreadPublicationLineage.repo_full_name)
            == feedback.repo_full_name.casefold(),
        )
        .limit(1)
    )
    return pending is not None


def normalized_feedback(feedback: UnverifiedFeedback) -> dict[str, Any]:
    """The only stored form of feedback; never the raw webhook body."""
    stored: dict[str, Any] = json.loads(json.dumps(asdict(feedback), default=str))
    return stored


def feedback_from_json(stored: dict[str, Any]) -> UnverifiedFeedback:
    data = dict(stored)
    try:
        data["delivery_id"] = uuid.UUID(data["delivery_id"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        return UnverifiedFeedback(**data)
    except (TypeError, ValueError, KeyError):
        raise FeedbackIgnored("stored_feedback_invalid") from None


def feedback_from_row(row: GitHubReviewFeedback) -> UnverifiedFeedback:
    return feedback_from_json(row.feedback)


def feedback_provenance(feedback: UnverifiedFeedback) -> dict[str, Any]:
    """The reviewer's verified feedback as the model sees it, shared with the factory arm."""
    provenance: dict[str, Any] = {
        "event": feedback.event,
        "url": feedback.url,
        "sender": feedback.sender_login,
        "body": feedback.body,
    }
    if feedback.path is not None:
        provenance.update(path=feedback.path, line=feedback.line, review_id=feedback.review_id)
    return provenance


def review_reply_handle(context: ReviewContext) -> ReplyHandle:
    """Reply where the conversation came from, on its captured binding."""
    binding = context.binding
    if context.origin_adapter == BUILTIN_CLUSTER_MESSAGE_ADAPTER:
        # The relay refuses a post without a session ref. The original CLI
        # session is long gone, so this turn gets a fresh bucket of its own.
        return ReplyHandle(
            kind=binding.kind,
            channel=binding.address,
            placeholder=str(uuid.uuid4()),
            endpoint=None,
            adapter=BUILTIN_CLUSTER_MESSAGE_ADAPTER,
        )
    return ReplyHandle(
        kind=binding.kind,
        channel=binding.address,
        placeholder=None,
        endpoint=binding.endpoint,
        adapter=binding.adapter,
    )


def review_turn(feedback: UnverifiedFeedback, context: ReviewContext) -> QueuedTurn:
    provenance = feedback_provenance(feedback)
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
        reply_handle=review_reply_handle(context),
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
            feedback=normalized_feedback(feedback),
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


# Held feedback lives in Valkey, not SQL, so the stable train needs no
# migration (#2962). Losing Valkey loses held reviews, which degrades to the
# pre-#2962 behavior: that review is never admitted and must be re-posted.
_HELD_INDEX = "curie:github-review:held"


def _held_key(event_id: str) -> str:
    return f"{_HELD_INDEX}:{event_id}"


def _held_deliveries_key(event_id: str) -> str:
    # Every delivery header GitHub sent for this one event; each has a receipt.
    return f"{_HELD_INDEX}:{event_id}:deliveries"


async def hold_feedback(
    valkey: redis.Redis,
    feedback: UnverifiedFeedback,
    *,
    traceparent: str | None,
    settings: Settings,
) -> None:
    """Retain feedback whose lineage awaits identity; a repeat is a no-op."""
    now = time.time()
    record = {
        "feedback": normalized_feedback(feedback),
        "traceparent": canonicalize_traceparent(traceparent),
        "attempts": 0,
        "held_at": now,
        "next_attempt_at": now,
    }
    ttl = max(1, int(2 * settings.github_review_identity_hold_s))
    await valkey.set(_held_key(feedback.event_id), json.dumps(record), nx=True, ex=ttl)
    deliveries = _held_deliveries_key(feedback.event_id)
    await valkey.sadd(deliveries, str(feedback.delivery_id))
    await valkey.expire(deliveries, ttl, nx=True)
    await valkey.zadd(_HELD_INDEX, {feedback.event_id: now}, nx=True)


async def replay_held_feedback(
    sessionmaker: async_sessionmaker[AsyncSession],
    valkey: redis.Redis,
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    repository_id: int | None = None,
    pr_number: int | None = None,
) -> list[str]:
    """Re-run admission for held feedback and settle its delivery receipt.

    Without a PR filter only entries whose backoff has elapsed are tried. With
    one, the PR's entries are tried at once because its identity was just
    recorded. Returns admitted event ids that still need enqueueing.
    """
    targeted = repository_id is not None and pr_number is not None
    now = time.time()
    raw = await (
        valkey.zrange(_HELD_INDEX, 0, -1)
        if targeted
        else valkey.zrangebyscore(_HELD_INDEX, "-inf", now, start=0, num=100)
    )
    window = settings.github_review_identity_hold_s
    if not targeted:
        await _reject_orphaned_held_receipts(sessionmaker, 2 * window)
    admitted: list[str] = []
    for member in raw:
        event_id = member.decode() if isinstance(member, bytes) else str(member)
        stored = await valkey.get(_held_key(event_id))
        if stored is None:
            # The key expired or Valkey lost it. Its receipts are rejected by
            # _reject_orphaned_held_receipts once they are past the key's TTL.
            await valkey.zrem(_HELD_INDEX, event_id)
            await valkey.delete(_held_deliveries_key(event_id))
            continue
        record = json.loads(stored)
        data = record["feedback"]
        if targeted and (
            data.get("repository_id") != repository_id or data.get("pr_number") != pr_number
        ):
            continue
        delivery_ids = {
            uuid.UUID(m.decode() if isinstance(m, bytes) else str(m))
            for m in await valkey.smembers(_held_deliveries_key(event_id))
        }
        outcome: str
        async with sessionmaker() as session, session.begin():
            # Receipt locks serialize replayers and a same-header redelivery.
            # Skip the event while any of its receipts is busy.
            audits: list[GitHubReviewDelivery] = []
            busy = False
            for delivery_id in sorted(delivery_ids):
                audit = await session.scalar(
                    select(GitHubReviewDelivery)
                    .where(GitHubReviewDelivery.delivery_id == delivery_id)
                    .with_for_update(skip_locked=True)
                )
                if audit is not None:
                    audits.append(audit)
                elif await session.get(GitHubReviewDelivery, delivery_id) is not None:
                    busy = True
                    break
            if busy:
                continue
            age = time.time() - float(record["held_at"])
            try:
                feedback = feedback_from_json(data)
                row, created = await admit_feedback(
                    session,
                    feedback,
                    settings=settings,
                    client=client,
                    traceparent=record["traceparent"],
                )
            except FeedbackHeld:
                if age >= window:
                    _settle_held(audits, "rejected", "lineage_absent_or_ambiguous")
                    outcome = "drop"
                else:
                    outcome = "retry"
            except FeedbackUnavailable as exc:
                # Provider outage is transient; the key's TTL bounds retention.
                if age >= 2 * window:
                    _settle_held(audits, "rejected", exc.code)
                    outcome = "drop"
                else:
                    outcome = "retry"
            except FeedbackIgnored as exc:
                _settle_held(audits, "rejected", exc.code)
                outcome = "drop"
            else:
                _settle_held(audits, "accepted", event_id=row.event_id)
                outcome = "drop"
                if created or row.status == "waiting":
                    admitted.append(row.event_id)
        # After commit: a lost delete only causes an idempotent re-admission.
        if outcome == "drop":
            await valkey.delete(_held_key(event_id), _held_deliveries_key(event_id))
            await valkey.zrem(_HELD_INDEX, event_id)
        else:
            record["attempts"] = int(record["attempts"]) + 1
            record["next_attempt_at"] = time.time() + min(
                60, 5 * 2 ** min(record["attempts"] - 1, 4)
            )
            await valkey.set(_held_key(event_id), json.dumps(record), xx=True, keepttl=True)
            await valkey.zadd(_HELD_INDEX, {event_id: record["next_attempt_at"]}, xx=True)
    return admitted


def _settle_held(
    audits: list[GitHubReviewDelivery],
    status: str,
    reason: str | None = None,
    *,
    event_id: str | None = None,
) -> None:
    for audit in audits:
        if audit.status in {"pending", "retryable"}:
            settle_review_delivery(audit, status, reason, event_id=event_id)


async def _reject_orphaned_held_receipts(
    sessionmaker: async_sessionmaker[AsyncSession], older_than_s: float
) -> None:
    """Settle held receipts whose Valkey hold can no longer exist.

    A hold key lives at most ``older_than_s`` from its first delivery, so a
    receipt still waiting past that age lost its hold to TTL or to Valkey.
    """
    async with sessionmaker() as session, session.begin():
        stale = await session.scalars(
            select(GitHubReviewDelivery)
            .where(
                GitHubReviewDelivery.status == "retryable",
                GitHubReviewDelivery.reason == "lineage_identity_pending",
                GitHubReviewDelivery.created_at
                <= func.now() - timedelta(seconds=older_than_s),
            )
            .limit(100)
            .with_for_update(skip_locked=True)
        )
        for audit in stale:
            settle_review_delivery(audit, "rejected", "lineage_absent_or_ambiguous")


class GitHubReviewReconciler:
    """SQL outbox to the existing atomic receipt + bounded runs consumer."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        valkey: redis.Redis,
        settings: Settings,
        client: httpx.AsyncClient,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._valkey = valkey
        self._settings = settings
        self._client = client

    async def replay_held(self, *, repository_id: int, pr_number: int) -> int:
        """Admit and enqueue one PR's held feedback right after identity lands."""
        admitted = await replay_held_feedback(
            self._sessionmaker,
            self._valkey,
            self._settings,
            self._client,
            repository_id=repository_id,
            pr_number=pr_number,
        )
        enqueued = 0
        for event_id in admitted:
            enqueued += await self.reconcile_once(event_id)
        return enqueued

    async def reconcile_once(self, event_id: str | None = None) -> int:
        if event_id is None:
            # Admitted rows are waiting outbox rows that this same pass enqueues.
            await replay_held_feedback(
                self._sessionmaker, self._valkey, self._settings, self._client
            )
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
    client: httpx.AsyncClient,
) -> uuid.UUID:
    """Freshly verify provider truth, then reserve the same DB generation/head.

    The worker calls this only after observing an idle runner under its route
    lock. The route applies a short overall control-plane deadline around this
    transaction so provider or database uncertainty stays retryable and cannot
    authorize a late model run.
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
    feedback, context = await validate_stored_context(session, row, settings)
    deployment = await session.get(Deployment, deployment_id)
    if deployment is None or deployment.agent_id != row.agent_id or deployment.status != "active":
        raise FeedbackIgnored("deployment_no_longer_authorized")
    if (
        row.lineage_version != expected_lineage_version
        or context.lineage.head_sha != expected_head_sha
    ):
        raise FeedbackIgnored("binding_or_lineage_changed")
    verified_head = await verify_feedback_truth(
        feedback,
        context.truth,
        settings=settings,
        client=client,
    )
    if verified_head != expected_head_sha:
        raise FeedbackIgnored("stale_feedback_head")
    try:
        reservation, locked_lineage, _ = await crud.reserve_review_revision(
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
    # reserve_review_revision refreshes and locks the lineage and its captured
    # binding. Compare the provider result and caller snapshot again only after
    # those locks are held; any mismatch rolls this transaction back.
    if (
        locked_lineage.id != context.lineage.id
        or locked_lineage.version != expected_lineage_version
        or locked_lineage.head_sha != expected_head_sha
        or locked_lineage.head_sha != verified_head
    ):
        raise FeedbackIgnored("binding_or_lineage_changed")
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
