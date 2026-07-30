"""Durable approvals: create, inspect, and resolve-once (#244, ADR-0010).

The worker creates a record when a run ends awaiting-approval and suspends the
session; resolving the record enqueues the resume turn back onto the runs
stream, so the whole pause/resume survives every component restarting. The
resolve endpoint owns the claim race: a conditional UPDATE picks exactly one
winner, losers get 409 with who resolved it, and a past-SLA record flips to
expired (410) instead of resolving.

Authorization today is the shared API key, like every router. WHO may resolve
(channel membership, self-approval block) is the server-side authorizer of
#246 and slots in at this endpoint -- the decision point is deliberately here,
on the server that owns the record, never inside the sandbox.
"""

import logging
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.exc import IntegrityError

from .. import crud
from ..auth import require_api_key
from ..authorizer import authorize_approval
from ..config import get_settings
from ..deps import ApproverSetSelectorDep, ResumeQueueDep, SessionDep
from ..models import Approval, ApprovalStatus
from ..resumequeue import build_expiry_resume_turn, build_resume_turn
from ..schemas import ApprovalAuditOut, ApprovalOut, ApprovalResolve
from ..wirebody import ApprovalRequestBody

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/approvals", tags=["approvals"], dependencies=[Depends(require_api_key)])


def _expired(approval: Approval) -> bool:
    """True when a pending record's SLA has passed (naive-UTC comparison,
    matching the DateTime columns)."""

    return approval.expires_at is not None and approval.expires_at <= datetime.now(UTC).replace(
        tzinfo=None
    )


@router.post("", response_model=ApprovalOut, status_code=status.HTTP_201_CREATED)
async def create_approval(
    data: ApprovalRequestBody, session: SessionDep, response: Response
) -> ApprovalOut:
    """Create a pending approval; idempotent on ``dedupe_key``.

    A redelivered worker turn that re-requests the same approval gets the
    existing record back (200) instead of forking a second pending record for
    one human decision.
    """

    try:
        approval = await crud.create_approval(session, data)
    except IntegrityError as exc:
        await session.rollback()
        existing = await crud.get_approval_by_dedupe_key(session, data.dedupe_key)
        if existing is None:  # raced with a delete; surface the conflict as-is
            raise HTTPException(
                status.HTTP_409_CONFLICT, "approval violates a uniqueness constraint"
            ) from exc
        response.status_code = status.HTTP_200_OK
        return ApprovalOut.model_validate(existing)
    return ApprovalOut.model_validate(approval)


@router.get("", response_model=list[ApprovalOut])
async def list_approvals(
    session: SessionDep,
    status_filter: str | None = None,
    agent_id: uuid.UUID | None = None,
    conversation_id: str | None = None,
    limit: int = 50,
) -> list[ApprovalOut]:
    approvals = await crud.list_approvals(
        session,
        status=status_filter,
        agent_id=agent_id,
        conversation_id=conversation_id,
        limit=min(max(limit, 1), 200),
    )
    return [ApprovalOut.model_validate(a) for a in approvals]


@router.get("/{approval_id}", response_model=ApprovalOut)
async def get_approval(approval_id: uuid.UUID, session: SessionDep) -> ApprovalOut:
    approval = await crud.get_approval(session, approval_id)
    if approval is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "approval not found")
    return ApprovalOut.model_validate(approval)


@router.get("/{approval_id}/audit", response_model=list[ApprovalAuditOut])
async def get_approval_audit(approval_id: uuid.UUID, session: SessionDep) -> list[ApprovalAuditOut]:
    """The approval's audit trail (#247), oldest first: every resolution
    attempt with the authorizer snapshot that counted or refused it."""

    approval = await crud.get_approval(session, approval_id)
    if approval is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "approval not found")
    entries = await crud.list_approval_audit(session, approval_id)
    return [ApprovalAuditOut.model_validate(e) for e in entries]


@router.post("/{approval_id}/resolve", response_model=ApprovalOut)
async def resolve_approval(
    approval_id: uuid.UUID,
    data: ApprovalResolve,
    session: SessionDep,
    resume_queue: ResumeQueueDep,
    approver_sets: ApproverSetSelectorDep,
) -> ApprovalOut:
    """Claim the resolution (resolve-once) and wake the suspended session.

    The authorizer runs first, server-side (#246): self-approval is blocked and
    the route's approvers decide -- an explicit user list, a Slack user group,
    or (declaring none) the card channel's members (#420); a denied actor gets
    403 with the reason. Then exactly one authorized resolver wins
    the conditional UPDATE; a loser gets 409 naming who resolved it, and a
    past-SLA record flips to expired and returns 410 while still enqueuing the
    expiry resume turn so the suspended session wakes down its timeout branch.
    The winner's response is sent only after the resume turn is enqueued.
    """

    approval = await crud.get_approval(session, approval_id)
    if approval is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "approval not found")

    # The route binding is read fresh at resolve time (#420), so revoking an
    # approver takes effect on the next click rather than at the next restart.
    binding = await crud.get_approval_route_binding(session, approval)
    approver_set = approver_sets(approval, binding)
    # Release the pooled DB connection before the membership lookup. A
    # group-bound set performs a Slack HTTP call (up to the client timeout), and
    # holding the read transaction's connection across it lets a Slack outage
    # plus concurrent clicks exhaust the pool and stall unrelated routes (#420).
    # The reads above are complete; expire_on_commit is False (create_sessionmaker)
    # so `approval` stays usable, and the CAS/audit below check out a fresh
    # connection, each committing itself as they already do.
    await session.commit()
    authorizer_name, decision = await authorize_approval(
        approval,
        data.resolved_by,
        data.actor_channel,
        approver_set=approver_set,
    )

    async def _audit(action: str, *, authorized: bool, reason: str | None) -> None:
        # The audit log (#247): every authorization-relevant event, with the
        # authorizer snapshot that counted (or refused) the actor, and the
        # membership evidence it decided on (#420).
        await crud.append_approval_audit(
            session,
            approval_id=approval_id,
            action=action,
            actor=data.resolved_by,
            actor_channel=data.actor_channel,
            decision=data.decision,
            authorizer=authorizer_name,
            authorized=authorized,
            reason=reason,
            evidence=decision.evidence,
        )

    if not decision.allowed:
        await _audit("denied", authorized=False, reason=decision.reason)
        raise HTTPException(status.HTTP_403_FORBIDDEN, decision.reason)

    if approval.status == ApprovalStatus.pending and _expired(approval):
        expired = await crud.expire_approval(session, approval_id)
        # None means a concurrent resolution won the CAS before the expiry did;
        # fall through to the claim below, which will lose and report the winner.
        if expired is not None:
            # Read expires_at into a plain value up front: the except below now
            # rolls back, which expires every ORM instance in this session, so
            # reading it off the record afterwards for the 410 message would
            # trigger an implicit reload (MissingGreenlet under the async
            # session) and turn the owed 410 back into a 500.
            expires_at = expired.expires_at
            await _audit(
                "expired",
                authorized=True,
                reason=f"approval expired at {expires_at}",
            )
            # This resolver lost the SLA (it still gets 410), but the session is
            # suspended and must be woken down its timeout branch. This resolver
            # won the expiry CAS (expire_approval returned non-None), so it is
            # the only writer that enqueues for this approval; a sweeper racing
            # the same flip gets None back and skips it. The shared
            # resume_event_id via build_expiry_resume_turn only guards against a
            # redelivery of an already-finished turn re-running; it is the CAS,
            # not the shared key, that keeps this wakeup single.
            try:
                await resume_queue.enqueue(build_expiry_resume_turn(expired))
                # Enqueue-first-then-mark (#418): only a wake that reached the
                # stream is written off, so a failure below leaves resumed_at
                # NULL and the reconciler re-enqueues it past its grace horizon.
                # This is the sole recovery path for an expiry wake -- a flipped
                # record is no longer pending, so no later sweep re-selects it.
                await crud.mark_approval_resumed(session, approval_id)
            except Exception:
                # Reset the session before raising: the mark inside the try is a
                # DB write, so its failure leaves this session in
                # PendingRollbackError until dependency teardown. Nothing
                # committed is discarded -- expire_approval and the audit each
                # commit themselves above, so the rollback only clears the failed
                # mark's aborted transaction. Mirrors the sweeper's except.
                await session.rollback()
                # A queue blip must not turn the 410 into a 500; the flip is
                # already committed, so report the expiry regardless. Unlike the
                # resolve path below, this branch never re-raises when the
                # reconciler is disabled: a 410 claims no delivery, it reports
                # the true fact that the approval expired, and a 500 here would
                # misinform the resolver about an outcome that did happen.
                #
                # The log must not name the enqueue as the failure: this except
                # also catches a failed mark, in which case the wake DID reach
                # the stream. An operator reads this line while deciding whether
                # a session is stranded, so it states the uncertainty rather than
                # guessing.
                retry = (
                    "the reconciler will re-enqueue it past its grace horizon "
                    "(a redundant wake if it did land, which is the safe "
                    "direction)"
                    if get_settings().resume_reconciler_enabled
                    else "nothing will retry it (resume reconciler disabled) "
                    "and the session wakeup may be lost"
                )
                logger.exception(
                    "expiry wakeup incomplete for approval %s on the resolve "
                    "path; the resume turn may or may not have reached the "
                    "stream, so %s",
                    approval_id,
                    retry,
                )
            raise HTTPException(
                status.HTTP_410_GONE,
                f"approval expired at {expires_at} and can no longer be resolved",
            )

    claimed = await crud.claim_approval_resolution(
        session,
        approval_id,
        decision=data.decision,
        resolved_by=data.resolved_by,
        note=data.note,
    )
    if claimed is None:
        current = await crud.get_approval(session, approval_id)
        if current is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "approval not found")
        if current.status == ApprovalStatus.expired:
            raise HTTPException(
                status.HTTP_410_GONE,
                "approval expired and can no longer be resolved",
            )
        await _audit(
            "race_lost",
            authorized=True,
            reason=f"already resolved by {current.resolved_by} ({current.status})",
        )
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"already resolved by {current.resolved_by} ({current.status})",
        )

    await _audit("resolved", authorized=True, reason=decision.reason or None)

    # Wake the suspended session: the resume turn rides the normal runs stream,
    # so the kernel's ordinary consume -> claim path rehydrates the thread
    # (ADR-0003) and delivers the decision. The turn's event_id is deterministic
    # per approval, so the worker's done-marker absorbs a duplicate enqueue.
    # If the enqueue fails (a Valkey blip), the resolution still committed (CAS
    # won, audit written): return 200 and leave resumed_at NULL so the reconciler
    # (#411) backstops the failed enqueue on its next pass. Raising here would
    # dead-end the client into the 409-forever branch on retry.
    # BUT the 200-and-defer contract is only safe when a reconciler will recover
    # the owed wake. With resume_reconciler_enabled=false there is no backstop, so
    # a 200 would silently strand the suspended session -- re-raise instead, so the
    # failure surfaces as a 500 the caller can see (the CAS/audit already committed).
    try:
        stream_id = await resume_queue.enqueue(build_resume_turn(claimed))
    except Exception:  # noqa: BLE001
        logger.warning(
            "approval %s %s by %s; resume enqueue failed, reconciler will retry",
            approval_id,
            claimed.status,
            claimed.resolved_by,
            exc_info=True,
        )
        if not get_settings().resume_reconciler_enabled:
            raise
        return ApprovalOut.model_validate(claimed)
    await crud.mark_approval_resumed(session, approval_id)
    logger.info(
        "approval %s %s by %s; resume turn enqueued (%s)",
        approval_id,
        claimed.status,
        claimed.resolved_by,
        stream_id,
    )
    return ApprovalOut.model_validate(claimed)
