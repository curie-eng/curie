"""Argument-bound remediation approvals, resolved without a model.

@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-16 @spec AUTOMATED-REMEDIATION-13

AUTOMATED-REMEDIATION-15: a nomination that is well formed but not admitted
raises one ``Approval`` of ``purpose`` ``remediation``
(``request_remediation_approval``, called by admission). The approval binds the
call: ``granted_tool`` ``mcp__<connector>__<tool>`` of the declared action and
``granted_arguments`` the nomination's canonical arguments; its reply fields
come from the protected delivery's ``QueuedTurn`` and never its text, its author
is the policy reference and its expiry is the policy's ``approval_ttl_seconds``.
Deduplication is on the nomination rows: while an approval raised for the same
agent, hook, action and ``arguments_sha256`` is pending, a further identical
nomination attaches to it (``remediation_approval_requests`` counts it and
keeps the newest ``event_id``) and raises no new card.

AUTOMATED-REMEDIATION-16: the resolve route keeps its authentication, approver
set and compare and set, and for this purpose owes no model wake. Before the
claim it asks ``resolution_refusal``: ``policy_changed`` when the current
generation no longer declares the action with the same connector and tool, and
``arguments_mismatch`` when the approval row's tool or argument digest no longer
equals the nomination's. After an approving claim, ``execute_approved`` moves
the raising nomination to ``approved`` and every attached one to ``finished``
and creates the one forward execution through
``remediation_forward.create_remediation_forward`` (AUTOMATED-REMEDIATION-13).
Attached nominations never execute: only the raising nomination holds the
``approved`` state the approval authority requires, so an approval yields at
most one execution. ``settle_remediation_approval`` ends every nomination of a
rejected or expired approval with that outcome.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final

from aci_protocol.turn import QueuedTurn
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .action_forward import ForwardCreated, ForwardRefused, arguments_sha256
from .models import (
    Agent,
    Approval,
    ApprovalStatus,
    RemediationApprovalRequest,
    RemediationNomination,
    RemediationPolicy,
    RemediationPolicyGeneration,
)
from .remediation_codes import APPROVAL_REASONS
from .remediation_forward import (
    APPROVAL_AUTHORITY,
    authority_generation,
    create_remediation_forward,
    declared_action,
    policy_ref,
)
from .remediation_policy_document import APPROVAL_TTL_SECONDS_DEFAULT

logger = logging.getLogger(__name__)

REMEDIATION_PURPOSE: Final = "remediation"
DEDUPE_PREFIX: Final = "remediation:"

ARGUMENTS_MISMATCH: Final = "arguments_mismatch"
POLICY_CHANGED: Final = "policy_changed"
AUTHORITY_UNAVAILABLE: Final = "authority_unavailable"

# A nomination admission may still send to approval: one not yet decided, or
# one a ``not_reversible_now`` refusal returned to approval with no approval yet.
_REQUESTABLE_STATES: Final = frozenset(
    {"received", "precondition_pending", "admitted", "approval_requested"}
)
_APPROVAL_REQUESTED: Final = "approval_requested"
_SUMMARY_ARGUMENTS_MAX: Final = 400


class RemediationApprovalUnavailable(Exception):
    """The nomination cannot raise an approval (no action, no generation, decided)."""


@dataclass(frozen=True)
class RemediationApprovalRequested:
    """The approval a nomination raised or attached to.

    ``created`` is False when the nomination attached to a pending identical
    approval or the call replays one already recorded on the nomination.
    """

    approval_id: uuid.UUID
    created: bool


def dedupe_key(nomination_id: uuid.UUID) -> str:
    """``remediation:<nomination id>`` (AUTOMATED-REMEDIATION-15)."""

    return f"{DEDUPE_PREFIX}{nomination_id}"


def granted_tool(connector: str, tool: str) -> str:
    """``mcp__<connector>__<tool>``, the ledger's tool name for a connector call."""

    return f"mcp__{connector}__{tool}"


def _connector_tool(action: Mapping[str, Any] | None) -> tuple[str, str] | None:
    if action is None:
        return None
    connector, tool = action.get("connector"), action.get("tool")
    if not isinstance(connector, str) or not isinstance(tool, str):
        return None
    return connector, tool


def approval_ttl_seconds(document: Mapping[str, Any]) -> int:
    limits = document.get("limits")
    value = limits.get("approval_ttl_seconds") if isinstance(limits, Mapping) else None
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return APPROVAL_TTL_SECONDS_DEFAULT


def route_card_channel(agent: Agent | None, route: str | None) -> str | None:
    """The route's fixed resolution address, or None for the requesting surface."""

    routes = agent.approval_routes if agent is not None else None
    binding = routes.get(route) if route and isinstance(routes, Mapping) else None
    resolution = binding.get("resolution") if isinstance(binding, Mapping) else None
    if isinstance(resolution, Mapping) and isinstance(resolution.get("address"), str):
        return str(resolution["address"])
    return None


def _summary(nomination: RemediationNomination, connector: str, tool: str) -> str:
    """Platform text naming the bound call; never the turn's text or the reason."""

    arguments = nomination.arguments or ""
    if len(arguments) > _SUMMARY_ARGUMENTS_MAX:
        arguments = arguments[: _SUMMARY_ARGUMENTS_MAX - 1] + "…"
    return (
        f"Remediation {nomination.action} on {nomination.target or 'its target'}: "
        f"{connector} {tool} {arguments}"
    )


async def _lock_identity(
    session: AsyncSession, agent_id: uuid.UUID, hook: str, action: str | None, sha: str | None
) -> None:
    """Serialize raise-or-attach with resolution for one (agent, hook, action, digest).

    Held for the transaction. An attach holds it from reading the pending
    approval to committing; ending an approval's nominations takes it first, so
    a nomination that attached while the approval was being resolved is
    committed, and ended with the outcome, before the resolution finishes them.
    """

    identity = f"remediation-approval:{agent_id}:{hook}:{action}:{sha}"
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
        {"identity": identity},
    )


async def _pending_identical(
    session: AsyncSession, nomination: RemediationNomination
) -> RemediationApprovalRequest | None:
    """The pending, unexpired approval raised for this nomination's identity."""

    found: RemediationApprovalRequest | None = await session.scalar(
        select(RemediationApprovalRequest)
        .join(Approval, Approval.id == RemediationApprovalRequest.approval_id)
        .where(
            RemediationApprovalRequest.agent_id == nomination.agent_id,
            RemediationApprovalRequest.hook == nomination.hook,
            RemediationApprovalRequest.action == nomination.action,
            RemediationApprovalRequest.arguments_sha256 == nomination.arguments_sha256,
            Approval.purpose == REMEDIATION_PURPOSE,
            Approval.status == ApprovalStatus.pending,
            or_(Approval.expires_at.is_(None), Approval.expires_at > func.now()),
        )
        .order_by(Approval.created_at.desc())
        .limit(1)
        .with_for_update(of=RemediationApprovalRequest)
    )
    return found


async def request_remediation_approval(
    session: AsyncSession,
    nomination_id: uuid.UUID,
    *,
    turn: QueuedTurn,
    check: str,
    observed: Any = None,
) -> RemediationApprovalRequested:
    """Raise, attach to, or replay the approval of a not-admitted nomination.

    @spec AUTOMATED-REMEDIATION-15. ``turn`` is the protected delivery's queued
    turn: only its conversation and reply handle are copied, never its text.
    ``check`` is the admission code that failed and ``observed`` the
    precondition read's value when one was taken; the card names both. Commits.
    Raises ``RemediationApprovalUnavailable`` having written nothing when the
    nomination names no call, no generation declares its action, or it was
    already decided.
    """

    if check not in APPROVAL_REASONS:
        raise ValueError(f"{check!r} is not a reason a nomination goes to approval")
    nomination = await session.scalar(
        select(RemediationNomination)
        .where(RemediationNomination.id == nomination_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if nomination is None or nomination.action is None or nomination.arguments is None:
        await session.rollback()
        raise RemediationApprovalUnavailable("no nomination names a call")
    if nomination.approval_id is not None:
        approval_id = nomination.approval_id
        await session.commit()
        return RemediationApprovalRequested(approval_id=approval_id, created=False)
    if nomination.state not in _REQUESTABLE_STATES or nomination.arguments_sha256 is None:
        await session.rollback()
        raise RemediationApprovalUnavailable(
            f"a nomination in state {nomination.state} raises no approval"
        )
    generation_number = await authority_generation(session, nomination, APPROVAL_AUTHORITY)
    generation = (
        await session.get(
            RemediationPolicyGeneration,
            (nomination.agent_id, nomination.hook, generation_number),
        )
        if generation_number is not None
        else None
    )
    document = generation.document if generation is not None else None
    bound = _connector_tool(
        declared_action(document, nomination.action) if isinstance(document, Mapping) else None
    )
    if generation_number is None or not isinstance(document, Mapping) or bound is None:
        await session.rollback()
        raise RemediationApprovalUnavailable("no policy generation declares the action")
    try:
        arguments = json.loads(nomination.arguments)
    except ValueError:
        arguments = None
    if not isinstance(arguments, dict):
        await session.rollback()
        raise RemediationApprovalUnavailable("the nominated arguments are malformed")
    handle = turn.reply_handle
    if handle is None:
        await session.rollback()
        raise RemediationApprovalUnavailable("the protected delivery has no reply handle")

    await _lock_identity(
        session,
        nomination.agent_id,
        nomination.hook,
        nomination.action,
        nomination.arguments_sha256,
    )
    if nomination.current_generation is None:
        # The generation the approval is bound under, so a later change to the
        # action is judged ``policy_changed`` against it (AUTOMATED-REMEDIATION-16).
        nomination.current_generation = generation_number
    pending = await _pending_identical(session, nomination)
    if pending is not None:
        approval_id = pending.approval_id
        pending.attached_count += 1
        pending.newest_event_id = nomination.event_id
        nomination.state = _APPROVAL_REQUESTED
        nomination.approval_id = approval_id
        await session.commit()
        return RemediationApprovalRequested(approval_id=approval_id, created=False)

    connector, tool = bound
    route = document.get("route") if isinstance(document.get("route"), str) else None
    agent = await session.get(Agent, nomination.agent_id)
    approval_id = uuid.uuid4()
    inserted = await session.scalar(
        insert(Approval)
        .values(
            id=approval_id,
            agent_id=nomination.agent_id,
            conversation_id=turn.conversation_id,
            author=policy_ref(
                nomination.agent_id, nomination.hook, generation_number, nomination.id
            ),
            summary=_summary(nomination, connector, tool),
            reply_kind=handle.kind,
            reply_channel=handle.channel,
            reply_placeholder=handle.placeholder,
            reply_endpoint=handle.endpoint,
            reply_adapter=handle.adapter,
            route=route,
            card_channel=route_card_channel(agent, route),
            dedupe_key=dedupe_key(nomination.id),
            status=ApprovalStatus.pending,
            expires_at=func.now() + timedelta(seconds=approval_ttl_seconds(document)),
            granted_tool=granted_tool(connector, tool),
            granted_arguments=arguments,
            purpose=REMEDIATION_PURPOSE,
        )
        .on_conflict_do_nothing(index_elements=[Approval.dedupe_key])
        .returning(Approval.id)
    )
    if inserted is None:
        # Only a row this nomination raised carries its dedupe key; adopt it.
        existing = await session.scalar(
            select(Approval.id).where(Approval.dedupe_key == dedupe_key(nomination.id))
        )
        if existing is None:
            await session.rollback()
            raise RemediationApprovalUnavailable("the approval could not be recorded")
        nomination.state = _APPROVAL_REQUESTED
        nomination.approval_id = existing
        await session.commit()
        return RemediationApprovalRequested(approval_id=existing, created=False)
    session.add(
        RemediationApprovalRequest(
            approval_id=approval_id,
            nomination_id=nomination.id,
            agent_id=nomination.agent_id,
            hook=nomination.hook,
            action=nomination.action,
            arguments_sha256=nomination.arguments_sha256,
            failed_check=check,
            observed=observed,
        )
    )
    nomination.state = _APPROVAL_REQUESTED
    nomination.approval_id = approval_id
    await session.commit()
    return RemediationApprovalRequested(approval_id=approval_id, created=True)


@dataclass(frozen=True)
class _Judged:
    """The raising nomination and the live generation an approval would execute under."""

    nomination: RemediationNomination
    live_generation: int


async def _judge(session: AsyncSession, approval: Approval) -> _Judged | ForwardRefused:
    """The AUTOMATED-REMEDIATION-16 checks against the rows as they are now."""

    request = await session.get(
        RemediationApprovalRequest, approval.id, populate_existing=True
    )
    nomination = (
        await session.get(RemediationNomination, request.nomination_id, populate_existing=True)
        if request is not None
        else None
    )
    if nomination is None or nomination.action is None or nomination.approval_id != approval.id:
        return ForwardRefused(AUTHORITY_UNAVAILABLE, "no nomination raised this approval")
    original_number = await authority_generation(session, nomination, APPROVAL_AUTHORITY)
    original = (
        await session.get(
            RemediationPolicyGeneration, (nomination.agent_id, nomination.hook, original_number)
        )
        if original_number is not None
        else None
    )
    live_number = await session.scalar(
        select(RemediationPolicy.generation).where(
            RemediationPolicy.agent_id == nomination.agent_id,
            RemediationPolicy.hook == nomination.hook,
        )
    )
    live = (
        await session.get(
            RemediationPolicyGeneration, (nomination.agent_id, nomination.hook, live_number)
        )
        if live_number is not None
        else None
    )
    bound = _connector_tool(
        declared_action(original.document, nomination.action) if original is not None else None
    )
    current = _connector_tool(
        declared_action(live.document, nomination.action) if live is not None else None
    )
    if live_number is None or current is None or current != bound:
        return ForwardRefused(
            POLICY_CHANGED, "the current policy generation no longer declares this action"
        )
    try:
        digest = arguments_sha256(approval.granted_arguments)
    except (TypeError, ValueError):
        digest = None
    if (
        approval.granted_tool != granted_tool(*current)
        or approval.granted_arguments is None
        or digest != nomination.arguments_sha256
    ):
        return ForwardRefused(
            ARGUMENTS_MISMATCH, "the approval no longer binds the nominated call"
        )
    return _Judged(nomination=nomination, live_generation=live_number)


async def resolution_refusal(session: AsyncSession, approval: Approval) -> ForwardRefused | None:
    """Why approving ``approval`` would execute nothing, or None when it may.

    @spec AUTOMATED-REMEDIATION-16. Read before the resolution claim, so a
    refused approval stays undecided and nothing is created. Ends the read
    (committing nothing, so loaded rows stay readable).
    """

    judged = await _judge(session, approval)
    await session.commit()
    return judged if isinstance(judged, ForwardRefused) else None


async def _lock_request(
    session: AsyncSession, approval_id: uuid.UUID
) -> RemediationApprovalRequest | None:
    """The approval's request row, after taking its identity's attach lock."""

    request = await session.get(RemediationApprovalRequest, approval_id, populate_existing=True)
    if request is not None:
        await _lock_identity(
            session, request.agent_id, request.hook, request.action, request.arguments_sha256
        )
    return request


async def _finish_nominations(
    session: AsyncSession,
    approval_id: uuid.UUID,
    *,
    raising: str,
    attached: str,
    raising_id: uuid.UUID | None,
    generation: int | None = None,
) -> None:
    """Move the raising nomination and every attached one off ``approval_requested``.

    The caller holds the identity lock (``_lock_request``).
    """

    values: dict[str, Any] = {"state": raising, "decided_at": func.now()}
    if generation is not None:
        values["current_generation"] = generation
    if raising_id is not None:
        await session.execute(
            update(RemediationNomination)
            .where(
                RemediationNomination.id == raising_id,
                RemediationNomination.approval_id == approval_id,
                RemediationNomination.state == _APPROVAL_REQUESTED,
            )
            .values(**values)
        )
    attached_where = [
        RemediationNomination.approval_id == approval_id,
        RemediationNomination.state == _APPROVAL_REQUESTED,
    ]
    if raising_id is not None:
        attached_where.append(RemediationNomination.id != raising_id)
    await session.execute(
        update(RemediationNomination)
        .where(*attached_where)
        .values(state=attached, decided_at=func.now())
    )


async def _finish_unexecuted(session: AsyncSession, raising_id: uuid.UUID, code: str) -> None:
    """End an ``approved`` raiser that has no execution, recording why in ``execution_code``."""

    await session.execute(
        update(RemediationNomination)
        .where(
            RemediationNomination.id == raising_id,
            RemediationNomination.state == "approved",
            RemediationNomination.execution_id.is_(None),
        )
        .values(state="finished", execution_code=code, decided_at=func.now())
    )


async def execute_approved(session: AsyncSession, approval_id: uuid.UUID) -> ForwardCreated:
    """Create the one forward execution of an approved remediation approval.

    @spec AUTOMATED-REMEDIATION-16 @spec AUTOMATED-REMEDIATION-13. The raising
    nomination becomes ``approved`` under the current generation and every
    attached nomination ``finished``; then the execution is built from the
    raising nomination's row. The checks of ``resolution_refusal`` are repeated
    under the nomination's lock; a refusal then finishes every nomination and
    raises ``ForwardRefused``. Replays adopt the same execution.
    """

    request = await _lock_request(session, approval_id)
    approval = await session.get(Approval, approval_id, populate_existing=True)
    if approval is None or request is None or approval.status != ApprovalStatus.approved:
        await session.rollback()
        raise ForwardRefused(AUTHORITY_UNAVAILABLE, "no approved remediation approval")
    raising_id = request.nomination_id
    await session.scalar(
        select(RemediationNomination.id)
        .where(RemediationNomination.id == raising_id)
        .with_for_update()
    )
    judged = await _judge(session, approval)
    if isinstance(judged, ForwardRefused):
        await _finish_nominations(
            session, approval_id, raising="finished", attached="finished", raising_id=raising_id
        )
        # A rerun after the claim may find the raiser already ``approved``.
        await _finish_unexecuted(session, raising_id, judged.code)
        await session.commit()
        raise judged
    await _finish_nominations(
        session,
        approval_id,
        raising="approved",
        attached="finished",
        raising_id=raising_id,
        generation=judged.live_generation,
    )
    await session.commit()
    try:
        return await create_remediation_forward(session, raising_id, approval_id=approval_id)
    except ForwardRefused as refused:
        # A refusal is final for this authority: end the raising nomination, so
        # reconciliation does not retry it. Any other error leaves it ``approved``
        # with no execution, which ``reconcile_remediation_approvals`` completes.
        await _finish_unexecuted(session, raising_id, refused.code)
        await session.commit()
        raise


async def settle_remediation_approval(
    session: AsyncSession, approval_id: uuid.UUID, outcome: str
) -> None:
    """End every nomination of a rejected or expired approval with its outcome.

    @spec AUTOMATED-REMEDIATION-16: "On ``rejected`` or ``expired`` it creates
    nothing and finishes the nomination and any nominations attached to it."
    Commits.
    """

    if outcome not in (ApprovalStatus.rejected, ApprovalStatus.expired):
        raise ValueError(f"{outcome!r} is not a terminal outcome without execution")
    request = await _lock_request(session, approval_id)
    raising_id = request.nomination_id if request is not None else None
    state = str(outcome)
    await _finish_nominations(
        session, approval_id, raising=state, attached=state, raising_id=raising_id
    )
    await session.commit()


_TERMINAL: Final = (ApprovalStatus.approved, ApprovalStatus.rejected, ApprovalStatus.expired)


async def reconcile_remediation_approvals(session: AsyncSession, *, limit: int = 100) -> int:
    """Complete the post-claim work of resolved remediation approvals.

    @spec AUTOMATED-REMEDIATION-16. The resolution or expiry claim commits
    before the nominations move and the execution is created; a failure between
    them leaves a resolved approval with a nomination still
    ``approval_requested``, or an ``approved`` raiser with no execution. Each
    pass finds those and runs the same step again: ``execute_approved`` adopts
    the one execution by its per-authority key, and ending nominations only
    moves rows still ``approval_requested``, so a replay changes nothing twice.
    Returns how many approvals it completed; a failure is logged and retried on
    the next pass.
    """

    owed = (
        select(RemediationNomination.id)
        .where(
            RemediationNomination.approval_id == Approval.id,
            or_(
                RemediationNomination.state == _APPROVAL_REQUESTED,
                (RemediationNomination.state == "approved")
                & RemediationNomination.execution_id.is_(None)
                & (RemediationNomination.id == RemediationApprovalRequest.nomination_id),
            ),
        )
        .exists()
    )
    rows = (
        await session.execute(
            select(Approval.id, Approval.status)
            .join(RemediationApprovalRequest, RemediationApprovalRequest.approval_id == Approval.id)
            .where(
                Approval.purpose == REMEDIATION_PURPOSE,
                Approval.status.in_(_TERMINAL),
                owed,
            )
            .order_by(Approval.resolved_at)
            .limit(limit)
        )
    ).all()
    await session.commit()
    completed = 0
    for approval_id, outcome in rows:
        try:
            if outcome == ApprovalStatus.approved:
                try:
                    await execute_approved(session, approval_id)
                except ForwardRefused as refused:
                    logger.warning(
                        "remediation approval %s refused %s on reconciliation",
                        approval_id,
                        refused.code,
                    )
            else:
                await settle_remediation_approval(session, approval_id, outcome)
            completed += 1
        except Exception:  # noqa: BLE001 - one approval must not stop the pass
            await session.rollback()
            logger.exception("remediation approval %s reconciliation failed", approval_id)
    return completed
