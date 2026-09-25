"""Break-glass approval recovery (#2753): report and recover.

An upgrade can strand an approval: the card is unreachable, or the row predates
a column the resume path now requires, and the suspended session waits forever.
This router is the operator's way out. It is deliberately a SEPARATE module from
``routers/approvals.py``: the ordinary resolve path is unchanged, and the two
surfaces are meant to stay readable apart.

**No new auth scheme and no new credential.** Authentication is the EXISTING
``require_platform_key`` (``apps/api/CLAUDE.md`` forbids another scheme without
an Accepted ADR). The actor is taken from the EXISTING ADR-0106 operator
principal for ATTRIBUTION only -- it names who acted in the audit row and is not
a second authority. ``authorize_approval`` is never called and route membership
is never consulted, so nothing here widens the ordinary resolve path: the same
operator principal a channel-membership route refuses with 403 recovers the very
same approval, and that route stays exactly as closed as it was.

**The grant is a plain setting**, ``Settings.approval_recovery_enabled``
(``CURIE_APPROVAL_RECOVERY_ENABLED`` / ``api.approvalRecovery.enabled``), OFF by
default. Enabling it lets ANY platform-key holder reject an approval
installation-wide, including on ordinarily resolvable approvals.
That blast radius is accepted explicitly rather than mitigated with a second
secret, because a recovery authority fenced to rows some predicate calls
unrecoverable is useless in exactly the situation nobody anticipated. What makes
the acceptance reviewable is atomicity: every effect and its audit row are one
transaction with one commit, so the trail can never be missing for an effect
that landed.

**Mount this router BEFORE ``routers/approvals.py``.** ``GET
/approvals/identity-report`` would otherwise be swallowed by ``GET
/approvals/{approval_id}`` and fail as a malformed uuid.
"""

import functools
import logging
import uuid
from collections.abc import Awaitable, Callable

from curie_telemetry import operation_span
from fastapi import APIRouter, Depends, HTTPException, status
from opentelemetry.trace import SpanKind
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from .. import crud
from ..approval_auth import ApprovalPrincipalDep
from ..auth import require_platform_key
from ..config import get_settings
from ..deps import ResumeQueueDep, SessionDep
from ..models import AgentChannel, Approval, ApprovalAuditEntry, ApprovalStatus
from ..resumequeue import approval_trace_context, build_resume_turn
from ..schemas import (
    ApprovalIdentityFactsOut,
    ApprovalIdentityReportOut,
    ApprovalRecover,
    ApprovalRecoveryOut,
    ApprovalReplyIdentityDeclaration,
)

logger = logging.getLogger(__name__)

#: The refusal an installation without the grant returns. It NAMES the setting
#: and the chart value, states the accepted blast radius, and says the path is
#: audited. It is deliberately worded so it can never be mistaken for a
#: membership or credential failure: an operator who read "not an approver" here
#: would go hunting through a route's approvers for a problem that is not there.
RECOVERY_DISABLED_DETAIL = (
    "approval recovery is not enabled on this installation; set "
    "api.approvalRecovery.enabled (CURIE_APPROVAL_RECOVERY_ENABLED) to grant "
    "it. Enabling it lets every platform-key holder reject an approval "
    "installation-wide, including on ordinarily resolvable approvals, and every "
    "use is audited."
)

#: The fact names the report and the audit evidence share. They describe what
#: was OBSERVED about a row. None of them says a row cannot be resolved.
FACT_ROUTE_DECLARED_BUT_UNBOUND = "route_declared_but_unbound"
FACT_CARD_IDENTITY_MISSING = "card_identity_missing"
FACT_REPLY_IDENTITY_UNRECONSTRUCTABLE = "reply_identity_unreconstructable"

#: The reply kind whose egress route is the worker's configured Slack origin
#: rather than an adapter-bearing binding (``agent_channels_route_pair_ck``).
_ROUTELESS_REPLY_KIND = "slack"

router = APIRouter(prefix="/approvals", tags=["approvals"])


# Postgres SQLSTATE for `deadlock_detected`, as `routers/agents.py` names it.
_DEADLOCK_DETECTED = "40P01"


def _deadlock_as_retryable_conflict[**P, R](
    handler: Callable[P, Awaitable[R]],
) -> Callable[P, Awaitable[R]]:
    """Answer a broken lock cycle with a retryable 409 instead of a 500.

    These routes read ``approvals`` then ``agent_channels``, and an identity
    migration fences ``agent_channels`` then ``approvals`` (see
    ``migration_fence.FENCED_TABLES``). No single order fits every writer, so
    that read can cycle with the fence; Postgres breaks the cycle by aborting
    one side with ``40P01``. When the aborted side is this request, nothing it
    meant to write has landed -- every recovery write is one transaction -- so
    the honest answer is "retry", not a server fault that tells an operator to
    stop retrying the one request that would now succeed.
    """

    @functools.wraps(handler)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await handler(*args, **kwargs)
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) != _DEADLOCK_DETECTED:
                raise
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                "an approval identity migration was fencing these tables and the "
                "database aborted this request to break a lock cycle. Nothing was "
                "changed -- retry it once the migration completes.",
            ) from exc

    return wrapped


async def require_recovery_enabled() -> None:
    """Refuse every recovery route unless the operator granted the path."""

    if not get_settings().approval_recovery_enabled:
        raise HTTPException(status.HTTP_403_FORBIDDEN, RECOVERY_DISABLED_DETAIL)


RecoveryEnabled = Depends(require_recovery_enabled)
PlatformKey = Depends(require_platform_key)


async def _binding_facts(session: SessionDep, agent_id: uuid.UUID | None) -> tuple[bool, set[str]]:
    """``(has any binding, the reply kinds it can authenticate egress for)``.

    Reads ``agent_channels`` and nothing else. That is the whole point: this
    surface has to answer on an installation whose schema is too old for the API
    to serve normally, so it never touches a column a later migration added.
    """

    if agent_id is None:
        return False, set()
    rows = (
        await session.execute(
            select(AgentChannel.kind, AgentChannel.adapter, AgentChannel.endpoint).where(
                AgentChannel.agent_id == agent_id
            )
        )
    ).all()
    return bool(rows), {
        kind
        for kind, adapter, endpoint in rows
        # Defensive, not reachable today: a Slack row's `adapter` is stored
        # as NULL (the default identity, ADR-0168 decision 3;
        # `route_identity`) until that decision's contract migration (#3100),
        # and 0024's `agent_channels_route_pair_ck`
        # ((endpoint IS NULL) = (adapter IS NULL)) makes "adapter set, no
        # endpoint" unstorable for ANY kind -- so a Slack row already reads as
        # bare truthiness excludes it, same as before. Stated explicitly
        # anyway, so a future write path that ever persisted a non-NULL
        # Slack identity here (a declared `'default'`, say) still would not
        # read as adapter-backed egress: this set answers "does a binding
        # authenticate the reply", and Slack's implicit worker-origin route
        # authenticates nothing a binding names. The one exception is the
        # pre-ADR custom-transport form (`endpoint is not None`): there
        # `adapter` IS a credential slug like any other kind's, so it keeps
        # today's meaning.
        if adapter and (kind != _ROUTELESS_REPLY_KIND or endpoint is not None)
    }


async def _observed_facts(session: SessionDep, approval: Approval) -> list[str]:
    """What is observably true about one row, in a stable order.

    Facts, not verdicts (and not a claim of unresolvability). Note what is
    ABSENT: there is no ``approver_set_malformed``. An approver set lives in
    ``agents.approval_routes``, a column this reader deliberately does not read
    so it survives a schema the fence refuses to serve, and inventing the fact
    from the binding table instead would report something other than its name.
    """

    has_binding, adapter_kinds = await _binding_facts(session, approval.agent_id)
    facts: list[str] = []
    if approval.route and not has_binding:
        # ADR-0123's fail-closed shape: a route the operator narrowed, with
        # nothing behind it to narrow to.
        facts.append(FACT_ROUTE_DECLARED_BUT_UNBOUND)
    if not (approval.reply_placeholder or "").strip():
        # No card to address a reply to.
        facts.append(FACT_CARD_IDENTITY_MISSING)
    if (
        approval.reply_kind != _ROUTELESS_REPLY_KIND
        and not approval.reply_adapter
        and approval.reply_kind not in adapter_kinds
    ):
        # A non-Slack reply must be authenticated with an egress identity, and
        # neither the row nor any adapter-bearing binding names one. Nothing in
        # the schema can reconstruct it, which is what the operator's
        # declaration document exists to supply.
        facts.append(FACT_REPLY_IDENTITY_UNRECONSTRUCTABLE)
    return facts


@router.get(
    "/identity-report",
    response_model=ApprovalIdentityReportOut,
    dependencies=[PlatformKey],
)
@_deadlock_as_retryable_conflict
async def identity_report(session: SessionDep) -> ApprovalIdentityReportOut:
    """Per-row facts for every pending approval, plus the declaration skeleton.

    A pure read, needing only the platform key, making NO Slack call, and
    excluding nothing: an ordinarily resolvable row appears here with an empty
    fact list rather than being filtered out, because a reporter that quietly
    dropped rows would be answering a question it cannot answer.
    """

    pending = list(
        await session.scalars(
            select(Approval)
            .where(Approval.status == ApprovalStatus.pending)
            .order_by(Approval.created_at, Approval.id)
        )
    )
    entries: list[ApprovalIdentityFactsOut] = []
    declarations: list[ApprovalReplyIdentityDeclaration] = []
    for approval in pending:
        facts = await _observed_facts(session, approval)
        entries.append(
            ApprovalIdentityFactsOut(
                id=approval.id,
                agent_id=approval.agent_id,
                status=approval.status,
                route=approval.route,
                reply_kind=approval.reply_kind,
                reply_adapter=approval.reply_adapter,
                reply_channel=approval.reply_channel,
                card_channel=approval.card_channel,
                has_reply_placeholder=bool((approval.reply_placeholder or "").strip()),
                created_at=approval.created_at,
                facts=facts,
            )
        )
        if FACT_REPLY_IDENTITY_UNRECONSTRUCTABLE in facts:
            declarations.append(ApprovalReplyIdentityDeclaration(approval_id=approval.id))
    return ApprovalIdentityReportOut(approvals=entries, declarations=declarations)


def _not_found() -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, "approval not found")


@router.post(
    "/{approval_id}/recover",
    response_model=ApprovalRecoveryOut,
    dependencies=[PlatformKey, RecoveryEnabled],
)
@_deadlock_as_retryable_conflict
async def recover_approval(
    approval_id: uuid.UUID,
    data: ApprovalRecover,
    session: SessionDep,
    resume_queue: ResumeQueueDep,
    principal: ApprovalPrincipalDep,
) -> ApprovalRecoveryOut:
    """Settle a stranded approval as ``rejected`` and wake its session.

    The compare-and-set and the audit append are one transaction with one
    commit, so there is no window in which the status flipped and the row
    explaining it does not exist. That same audit row carries the
    ``recovery_key``, which is what makes a retry a read: a key already recorded
    for this approval returns the recorded outcome and enqueues nothing. The resume then rides the
    ORDINARY path -- the same runs stream every other resolution uses -- because
    a break-glass wake that travelled a private route would be the one wake
    nobody could reason about.
    """

    approval = await crud.get_approval(session, approval_id)
    if approval is None:
        raise _not_found()
    recorded = await crud.find_recovery_audit(session, data.recovery_key)
    if recorded is not None:
        return _replayed_recovery(approval, recorded, data.recovery_key)
    stored_parent = approval_trace_context(approval)
    facts = await _observed_facts(session, approval)
    # Release the read transaction: the atomic function below opens one of its
    # own, and a session already inside a transaction cannot begin another.
    await session.commit()

    try:
        with operation_span(
            "curie.approval.recover",
            kind=SpanKind.INTERNAL,
            parent=stored_parent,
            attributes={"service.name": "curie-api", "operation": "recover"},
        ):
            recovered = await crud.recover_approval_atomic(
                session,
                approval_id,
                reason=data.reason,
                recovery_key=data.recovery_key,
                actor=principal.subject,
                actor_channel=principal.actor_channel,
                principal_kind=principal.kind,
                facts=facts,
            )
    except crud.PublicationSettlementConflict as exc:
        # The approval's publication moved between the read and the settlement.
        # The whole transaction rolled back, so the approval is untouched and a
        # retry is safe -- which is exactly what this 409 tells the operator.
        await session.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc

    if recovered is None:
        # The CAS lost. A concurrent request under the same key may have won
        # it, in which case this is a replay; otherwise the row was settled by
        # something else and this key never recorded an outcome.
        current = await crud.reread_approval(session, approval_id)
        if current is None:
            raise _not_found()
        recorded = await crud.find_recovery_audit(session, data.recovery_key)
        if recorded is not None:
            return _replayed_recovery(current, recorded, data.recovery_key)
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"approval is already {current.status} and was not recovered under this recovery_key",
        )

    logger.warning(
        "approval %s administratively recovered as rejected by %s (key %s): %s",
        approval_id,
        principal.subject,
        data.recovery_key,
        data.reason,
    )
    if recovered.purpose != "publication":
        # Publication outcomes are reported by the worker through the stored
        # reply route; no model wake is owed and none is enqueued.
        await resume_queue.enqueue(build_resume_turn(recovered), parent=stored_parent)
        await crud.mark_approval_resumed(session, approval_id)
    return _recovery_out(recovered, data.recovery_key)


def _recovery_out(approval: Approval, recovery_key: str) -> ApprovalRecoveryOut:
    return ApprovalRecoveryOut(
        approval_id=approval.id,
        status=approval.status,
        recovery_key=recovery_key,
        reason=approval.resolution_note,
        actor=approval.resolved_by,
        recovered_at=approval.resolved_at,
    )


def _replayed_recovery(
    current: Approval, recorded: ApprovalAuditEntry, recovery_key: str
) -> ApprovalRecoveryOut:
    """A key that already recorded an outcome is a replay or a reuse.

    The body is built entirely from the persisted row, so a retry renders the
    outcome that already happened rather than a second, subtly different one.
    """

    if recorded.approval_id == current.id:
        return _recovery_out(current, recovery_key)
    raise HTTPException(
        status.HTTP_409_CONFLICT,
        "recovery_key already records the outcome of another approval",
    )
