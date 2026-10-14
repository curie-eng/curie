"""Not verified: report, escalate, never undo automatically.

@spec AUTOMATED-REMEDIATION-19

docs/superpowers/specs/2026-10-07-automated-remediation.md, AUTOMATED-REMEDIATION-19.

Escalation (``escalate``). Called by the verifier in the transaction that
writes a remediation's outcome when that outcome is anything other than
``verified`` (``remediation_verifier._write_outcome`` and
``finish_unverified``), after the breaker opened
(AUTOMATED-REMEDIATION-11). It writes the one ``remediation_escalations`` row
of the nomination: the failure report the worker remediation loop delivers to
the policy's route and the delivery's thread. The outcome is written once, so a
nomination escalates once; a replayed deciding report adds nothing.

Undo offer. For a ``reversible`` action (read from the generation that
authorized the record) whose record is undoable now (the one derivation,
``action_undoable.undo_refusal``), the same transaction raises one approval on
the policy's route, bound to the restore of that record (``granted_tool``
``mcp__<connector>__restore``; the escalation row names the record), with the
policy's approval expiry and the reply surface of the protected delivery. It is
of purpose ``remediation`` (resolved without a model wake) and is told apart
from a forward remediation approval by its ``remediation-undo:`` dedupe key
(``is_undo_approval``) and the escalation row naming it; it has no
``remediation_approval_requests`` row, so the forward approval's card loop,
attach rule and reconciliation never see it. Approving it drives the undo ruling
(``routers/actions.py::rule_undo``) under the approving principal; nothing here
creates a restore or calls the ruling.

Authority-aware undo authorization (``undo_route_approval``). A ``policy``
record is authorized against the policy route's approver set: the selector is
asked about the approval this module would raise for it, so the ruling route
and the undo approval admit exactly the same principals.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .action_undoable import undo_refusal
from .config import get_settings
from .models import (
    Agent,
    AgentAction,
    Approval,
    ApprovalStatus,
    RemediationDeliverySurface,
    RemediationEscalation,
    RemediationNomination,
    RemediationPolicyGeneration,
)
from .remediation_approvals import (
    REMEDIATION_PURPOSE,
    approval_ttl_seconds,
    granted_tool,
    route_card_channel,
)
from .remediation_forward import POLICY_AUTHORITY, declared_action, policy_generation
from .storage import BundleStore, ObjectStore

logger = logging.getLogger(__name__)

UNDO_DEDUPE_PREFIX: Final = "remediation-undo:"
RESTORE_TOOL: Final = "restore"
VERIFIED: Final = "verified"
REVERSIBLE: Final = "reversible"
# The reply kind a route's fixed resolution address is posted on.
_SLACK: Final = "slack"


def undo_dedupe_key(nomination_id: uuid.UUID) -> str:
    """``remediation-undo:<nomination id>``: one undo approval per nomination."""

    return f"{UNDO_DEDUPE_PREFIX}{nomination_id}"


def is_undo_approval(approval: Approval) -> bool:
    """Whether ``approval`` is an undo approval ``escalate`` raised.

    Both fields are server-owned: no caller sets an approval's purpose, and
    only ``escalate`` mints a ``remediation-undo:`` key on one of purpose
    ``remediation``.
    """

    return approval.purpose == REMEDIATION_PURPOSE and approval.dedupe_key.startswith(
        UNDO_DEDUPE_PREFIX
    )


@dataclass(frozen=True)
class _Surface:
    kind: str
    channel: str
    endpoint: str | None
    adapter: str | None


def _route_of(generation: RemediationPolicyGeneration | None) -> str | None:
    document = generation.document if generation is not None else None
    route = document.get("route") if isinstance(document, Mapping) else None
    return route if isinstance(route, str) and route else None


async def _nomination_of(
    session: AsyncSession, action: AgentAction
) -> RemediationNomination | None:
    if action.nomination_id is None:
        return None
    nomination = await session.get(RemediationNomination, action.nomination_id)
    if nomination is None or nomination.agent_id != action.agent_id:
        return None
    return nomination


async def _surface(
    session: AsyncSession, action: AgentAction, card_channel: str | None
) -> _Surface | None:
    """The protected delivery's reply surface, else the route's fixed Slack address."""

    surface = (
        await session.get(RemediationDeliverySurface, action.delivery_event_id)
        if action.delivery_event_id is not None
        else None
    )
    if surface is not None and surface.agent_id == action.agent_id:
        return _Surface(
            kind=surface.reply_kind,
            channel=surface.reply_channel,
            endpoint=surface.reply_endpoint,
            adapter=surface.reply_adapter,
        )
    if card_channel is not None:
        return _Surface(kind=_SLACK, channel=card_channel, endpoint=None, adapter=None)
    return None


async def policy_route(
    session: AsyncSession, action: AgentAction
) -> tuple[RemediationNomination, RemediationPolicyGeneration, str] | None:
    """The nomination, generation and route that authorized a remediation record.

    The generation is the one the record's authority executed under
    (``remediation_forward.authority_generation``). None when the record names
    no nomination of its agent, no generation is readable or it names no route.
    """

    nomination = await _nomination_of(session, action)
    if nomination is None or action.authority_kind is None:
        return None
    generation = await policy_generation(session, nomination, action.authority_kind)
    route = _route_of(generation)
    if generation is None or route is None:
        return None
    return nomination, generation, route


async def undo_route_approval(session: AsyncSession, action: AgentAction) -> Approval | None:
    """The approval an undo of a ``policy`` record is authorized against, unsaved.

    @spec AUTOMATED-REMEDIATION-19: "a record with ``authority_kind`` ``policy``
    requires a principal in the policy route's approver set". The approver set
    selector reads an approval's route, card channel and reply surface, so this
    is the approval ``escalate`` would raise for the record, never added to the
    session. None when the record's policy route cannot be resolved, which
    admits nobody.
    """

    if action.authority_kind != POLICY_AUTHORITY or action.agent_id is None:
        return None
    found = await policy_route(session, action)
    if found is None:
        return None
    _, _, route = found
    agent = await session.get(Agent, action.agent_id)
    card_channel = route_card_channel(agent, route)
    surface = await _surface(session, action, card_channel)
    if surface is None:
        return None
    return Approval(
        agent_id=action.agent_id,
        route=route,
        card_channel=card_channel,
        reply_kind=surface.kind,
        reply_channel=surface.channel,
        reply_endpoint=surface.endpoint,
        reply_adapter=surface.adapter,
        purpose=REMEDIATION_PURPOSE,
    )


def _summary(nomination: RemediationNomination, connector: str) -> str:
    """Platform text naming the restore; never an argument value, state or reason."""

    return (
        f"Undo remediation {nomination.action} on {nomination.target or 'its target'}: "
        f"{connector} restore of the recorded prior state"
    )


async def _offer_undo(
    session: AsyncSession,
    store: ObjectStore,
    nomination: RemediationNomination,
    action: AgentAction,
) -> uuid.UUID | None:
    """Raise the undo approval of an undoable ``reversible`` record, or None."""

    if action.agent_id is None or action.connector is None or action.authority_kind is None:
        return None
    generation = await policy_generation(session, nomination, action.authority_kind)
    declared = (
        declared_action(generation.document, nomination.action)
        if generation is not None and nomination.action is not None
        else None
    )
    route = _route_of(generation)
    if declared is None or declared.get("reversibility") != REVERSIBLE or route is None:
        return None
    if await undo_refusal(session, store, action) is not None:
        return None
    agent = await session.get(Agent, action.agent_id)
    card_channel = route_card_channel(agent, route)
    surface = await _surface(session, action, card_channel)
    if surface is None:
        logger.warning(
            "remediation undo not offered: no reply surface nomination=%s", nomination.id
        )
        return None
    document: Mapping[str, Any] = generation.document if generation is not None else {}
    approval_id: uuid.UUID | None = await session.scalar(
        insert(Approval)
        .values(
            id=uuid.uuid4(),
            agent_id=action.agent_id,
            conversation_id=action.conversation_id,
            author=f"remediation-escalation:{nomination.id}",
            summary=_summary(nomination, action.connector),
            reply_kind=surface.kind,
            reply_channel=surface.channel,
            reply_endpoint=surface.endpoint,
            reply_adapter=surface.adapter,
            route=route,
            card_channel=card_channel,
            dedupe_key=undo_dedupe_key(nomination.id),
            status=ApprovalStatus.pending,
            expires_at=func.now() + timedelta(seconds=approval_ttl_seconds(document)),
            granted_tool=granted_tool(action.connector, RESTORE_TOOL),
            purpose=REMEDIATION_PURPOSE,
        )
        .on_conflict_do_nothing(index_elements=[Approval.dedupe_key])
        .returning(Approval.id)
    )
    return approval_id


async def escalate(
    session: AsyncSession,
    *,
    nomination_id: uuid.UUID,
    outcome: str,
    action_id: uuid.UUID | None,
    store: ObjectStore | None = None,
) -> None:
    """Write the escalation of a remediation whose outcome is not ``verified``.

    @spec AUTOMATED-REMEDIATION-19. Called in the transaction that writes the
    outcome, once (the caller writes the outcome only while none is written).
    Writes the report row and, for an undoable ``reversible`` record, the undo
    approval it offers. Creates no restore. Commits nothing.
    """

    if outcome == VERIFIED:
        return
    nomination = await session.get(RemediationNomination, nomination_id)
    if nomination is None:
        return
    escalation_id: uuid.UUID | None = await session.scalar(
        insert(RemediationEscalation)
        .values(
            id=uuid.uuid4(),
            agent_id=nomination.agent_id,
            nomination_id=nomination.id,
            action_id=action_id,
            outcome=outcome,
        )
        .on_conflict_do_nothing(index_elements=[RemediationEscalation.nomination_id])
        .returning(RemediationEscalation.id)
    )
    if escalation_id is None:
        return
    action = (
        await session.get(AgentAction, action_id, populate_existing=True)
        if action_id is not None
        else None
    )
    undo_approval_id = (
        await _offer_undo(session, store or BundleStore(get_settings()), nomination, action)
        if action is not None and action.agent_id == nomination.agent_id
        else None
    )
    if undo_approval_id is not None:
        await session.execute(
            update(RemediationEscalation)
            .where(RemediationEscalation.id == escalation_id)
            .values(undo_approval_id=undo_approval_id)
            .execution_options(synchronize_session=False)
        )
    logger.info(
        "remediation escalated nomination=%s outcome=%s undo_offered=%s",
        nomination.id,
        outcome,
        undo_approval_id is not None,
    )


async def undo_subject(session: AsyncSession, approval: Approval) -> AgentAction | None:
    """The ledger record an undo approval is bound to, through its escalation.

    None when no escalation names the approval, or the record it names is
    missing or belongs to another agent.
    """

    action_id = await session.scalar(
        select(RemediationEscalation.action_id).where(
            RemediationEscalation.undo_approval_id == approval.id,
            RemediationEscalation.agent_id == approval.agent_id,
        )
    )
    if action_id is None:
        return None
    action = await session.get(AgentAction, action_id, populate_existing=True)
    if action is None or action.agent_id != approval.agent_id:
        return None
    return action


__all__ = [
    "UNDO_DEDUPE_PREFIX",
    "escalate",
    "is_undo_approval",
    "policy_route",
    "undo_dedupe_key",
    "undo_route_approval",
    "undo_subject",
]
