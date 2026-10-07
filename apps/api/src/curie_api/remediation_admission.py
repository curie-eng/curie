"""Admission of remediation nominations: the order, limits, breaker and disarm.

@spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-9 @spec AUTOMATED-REMEDIATION-10
@spec AUTOMATED-REMEDIATION-11

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the maintainer
rulings of 2026-10-07 (the incident window, the administrative principal) and
executor amendment E8.

Admission (``admit_nominations``) runs inside the nomination route once the
rows are written. Each ``received`` nomination is evaluated in the order of
AUTOMATED-REMEDIATION-8 and the first failing check decides (check 1 is the
route's own ``remediation_disabled``):

2. the agent's kill switch, read through the key the worker reads: killed or
   unreadable ends the nomination ``refused`` ``agent_stopped`` (a stopped
   agent asks nobody);
3. the admitted generation is present and is the hook's current generation
   (AUTOMATED-REMEDIATION-4): ``generation_not_current``;
4. the current generation is armed: ``policy_disarmed``;
5. the action is a ``remediate`` action declared ``automatic``: ``not_automatic``;
6. its qualification record (``qualification_refusal``);
7. verifier independence against the in-force version
   (``remediation_verifier.independence_refusal``);
8. every argument within its declared set or range, and the target value a
   literal member of the target list: ``out_of_bounds``;
9. a ``reversible`` action's connector records the restore pair at the
   in-force digest (ACTION-EXECUTOR-13) and the version declares its sealing
   key (ACTION-EXECUTOR-16): ``not_reversible_now``;
10. no open breaker for the action's connector, tool and target key:
    ``breaker_open``;
11. the limits, counted from ``remediation_reservations`` and the ledger under a
    transaction-scoped advisory lock keyed by the agent, and the reservation
    written in the same transaction: ``turn_limit`` (one automatic action per
    turn), ``target_live`` (one live automatic action per target),
    ``incident_limit`` (the target's incident window), ``action_rate_limit`` and
    ``policy_rate_limit`` (rolling hour).

Checks 3 to 11 run in one transaction under that lock. Passing them, the same
transaction writes the reservation and one ``read`` execution of the declared
precondition (``remediation:<nomination id>:precondition``, the read connector
at its in-force digest, the declared tool, arguments and pointer, due now,
under the policy authority) and leaves the nomination ``precondition_pending``.
A database error on any read (the policy, a breaker, the lock, a capability
row) rolls it back and sends the nomination to approval
``admission_unreadable``: never to execution.

Check 12 (``precondition_ended``) runs when the read ends. A refused read is
``precondition_unavailable``. A sample is evaluated against the admitted
generation's precondition predicate: unsuccessful is
``precondition_unavailable``, unsatisfied ``precondition_not_met``. Satisfied,
checks 2 to 11 run again under the lock (this nomination's own reservation
excluded) and, in the same transaction, the nomination becomes ``admitted``
and its policy forward execution is created
(``remediation_forward.create_remediation_forward``). Any failure sends it to
approval with that check's reason (or ends it ``agent_stopped``).

A nomination sent to approval is ``approval_requested`` with its
``approval_reason`` and its reservation released, in one transaction; the
approval itself is then raised through
``remediation_approvals.request_remediation_approval`` with the delivery's
conversation and the agent's reply surface. When that cannot be done now (an
unreadable store), the nomination keeps its reason and no approval id, and
``reconcile_admissions`` raises it on a later pass.

The incident window (maintainer ruling, 2026-10-07) is derived from the ledger,
never from the alert body: a remediation's ledger record (automatic or
approved) on the same agent and target key whose verification has not finished,
or finished less than the window ago, where the window is the longest of the
default 3600 seconds, the evaluating policy's and the executing policy's
``incident_window_seconds``.

E8 (``authority_refusal``): the claim route asks, for a ``policy``-authorized
forward execution only, whether its nomination's admitted generation is still
current and armed and no breaker is open for its target key; otherwise it is
refused ``policy_changed`` before any sandbox claim and its nomination returns
to approval (``return_to_approval``).

Nothing here logs an argument, a target, a sample or a reason.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

from aci_protocol import QueuedTurn, ReplyHandle, ToolAccess, TurnSource
from channel_protocol import hook_conversation_id
from sqlalchemy import Integer, and_, cast, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from .action_forward import ForwardRefused
from .action_undoable import sealing_custody
from .killswitch import KillSwitch
from .models import (
    ActionExecution,
    AgentAction,
    ConnectorCapability,
    ExecutionKind,
    ExecutionState,
    RemediationDeliverySurface,
    RemediationNomination,
    RemediationNominationSubmission,
    RemediationPolicy,
    RemediationPolicyGeneration,
    RemediationReservation,
)
from .remediation_approvals import (
    RemediationApprovalUnavailable,
    request_remediation_approval,
)
from .remediation_forward import (
    IDEMPOTENCY_PREFIX,
    POLICY_AUTHORITY,
    create_remediation_forward,
    declared_action,
    in_force_connector_digest,
    nomination_for_execution,
    policy_ref,
)
from .remediation_limits import breaker_open, release_reservation
from .remediation_policy_document import (
    INCIDENT_WINDOW_SECONDS_MINIMUM,
    PER_POLICY_PER_HOUR_CEILING,
)
from .remediation_predicate import SATISFIED, UNSUCCESSFUL, evaluate_sample
from .remediation_reads import ReadRefused, scheduled_read
from .remediation_verifier import independence_refusal
from .storage import ObjectStore

logger = logging.getLogger(__name__)

# Nomination states (AUTOMATED-REMEDIATION-8).
RECEIVED: Final = "received"
REFUSED: Final = "refused"
PRECONDITION_PENDING: Final = "precondition_pending"
ADMITTED: Final = "admitted"
APPROVAL_REQUESTED: Final = "approval_requested"
# Live: an automatic action that has not finished verifying (AUTOMATED-REMEDIATION-10).
LIVE_STATES: Final = frozenset({PRECONDITION_PENDING, ADMITTED, "executing", "verifying"})

# Codes (tests/vectors/remediation-codes.json).
AGENT_STOPPED: Final = "agent_stopped"
GENERATION_NOT_CURRENT: Final = "generation_not_current"
POLICY_DISARMED: Final = "policy_disarmed"
NOT_AUTOMATIC: Final = "not_automatic"
QUALIFICATION_MISSING: Final = "qualification_missing"
QUALIFICATION_STALE: Final = "qualification_stale"
OUT_OF_BOUNDS: Final = "out_of_bounds"
NOT_REVERSIBLE_NOW: Final = "not_reversible_now"
BREAKER_OPEN: Final = "breaker_open"
TURN_LIMIT: Final = "turn_limit"
TARGET_LIVE: Final = "target_live"
INCIDENT_LIMIT: Final = "incident_limit"
ACTION_RATE_LIMIT: Final = "action_rate_limit"
POLICY_RATE_LIMIT: Final = "policy_rate_limit"
PRECONDITION_NOT_MET: Final = "precondition_not_met"
PRECONDITION_UNAVAILABLE: Final = "precondition_unavailable"
ADMISSION_UNREADABLE: Final = "admission_unreadable"
POLICY_CHANGED: Final = "policy_changed"
REPLY_SURFACE_UNAVAILABLE: Final = "reply_surface_unavailable"
UNKNOWN_ACTION: Final = "unknown_action"

# The precondition read's idempotency key: ``remediation:<nomination id>:precondition``.
_PRECONDITION: Final = "precondition"
_HOUR_SECONDS: Final = 3600
# A ``received`` nomination this old was left by a submission that did not
# finish admitting it; ``reconcile_admissions`` admits it.
_STRANDED_SECONDS: Final = 60

_ADMISSION_LOCK = text(
    "SELECT pg_advisory_xact_lock(hashtextextended('curie.remediation.admission:' || :agent, 0))"
)


# --------------------------------------------------------------------------- #
# Check 6: the qualification record (AUTOMATED-REMEDIATION-22)
# --------------------------------------------------------------------------- #


async def qualification_refusal(
    session: AsyncSession,
    agent_id: uuid.UUID,
    action: Mapping[str, Any],
    *,
    store: ObjectStore | None = None,
) -> str | None:
    """``qualification_missing``, ``qualification_stale`` or None (check 6).

    @spec AUTOMATED-REMEDIATION-8 (check 6) @spec AUTOMATED-REMEDIATION-22. An
    action with no ``qualification`` reference has no record. Qualification
    records are plan task 15 and no record store exists before it, so a
    reference names no record admission can find: every action is
    ``qualification_missing`` until task 15 replaces this lookup with the
    record's state and digest. Fails closed; never None today.
    """

    del session, agent_id, store  # read by task 15's record lookup
    if not action.get("qualification"):
        return QUALIFICATION_MISSING
    return QUALIFICATION_MISSING


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #


def precondition_key(nomination_id: uuid.UUID) -> str:
    """``remediation:<nomination id>:precondition``. @spec AUTOMATED-REMEDIATION-9."""

    return f"{IDEMPOTENCY_PREFIX}{nomination_id}:{_PRECONDITION}"


def precondition_nomination(key: str | None) -> uuid.UUID | None:
    """The nomination a precondition read's key names, or None for any other key."""

    if not key or not key.startswith(IDEMPOTENCY_PREFIX):
        return None
    parts = key.removeprefix(IDEMPOTENCY_PREFIX).split(":")
    if len(parts) != 2 or parts[1] != _PRECONDITION:
        return None
    try:
        return uuid.UUID(parts[0])
    except ValueError:
        return None


def _literal(value: Any, allowed: Any) -> bool:
    """A literal member: the same JSON type and value (``True`` is never ``1``)."""

    return type(value) is type(allowed) and value == allowed


def _number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def within_bounds(action: Mapping[str, Any], arguments: Mapping[str, Any]) -> bool:
    """Check 8: each argument in its declared set or range, the target a literal member.

    @spec AUTOMATED-REMEDIATION-8 (check 8) @spec AUTOMATED-REMEDIATION-10: "a
    value that is not a literal member is out of bounds". Compared exactly as
    the policy lists it: no normalization, case folding or numeric coercion.
    """

    schema = action.get("arguments")
    if not isinstance(schema, Mapping) or set(schema) != set(arguments):
        return False
    for key, spec in schema.items():
        value = arguments[key]
        if not isinstance(spec, Mapping):
            return False
        if "allowed" in spec:
            allowed = spec.get("allowed")
            if not isinstance(allowed, list) or not any(_literal(value, a) for a in allowed):
                return False
            continue
        minimum, maximum = spec.get("minimum"), spec.get("maximum")
        if not (_number(value) and _number(minimum) and _number(maximum)):
            return False
        if not minimum <= value <= maximum:
            return False
    target = action.get("target")
    if not isinstance(target, Mapping):
        return False
    argument, allowed = target.get("argument"), target.get("allowed")
    if not isinstance(argument, str) or argument not in arguments or not isinstance(allowed, list):
        return False
    return any(_literal(arguments[argument], a) for a in allowed)


def _limits(document: Mapping[str, Any]) -> Mapping[str, Any]:
    limits = document.get("limits")
    return limits if isinstance(limits, Mapping) else {}


def _positive(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return None
    return value


async def _stopped(kill_switch: KillSwitch, agent_id: uuid.UUID) -> bool:
    """Check 2: killed, or a switch that cannot be read. @spec AUTOMATED-REMEDIATION-8."""

    try:
        return await kill_switch.is_killed(agent_id)
    except Exception:  # noqa: BLE001 - an unreadable switch is a stopped agent
        return True


# --------------------------------------------------------------------------- #
# Checks 3 to 11 under the admission lock
# --------------------------------------------------------------------------- #


@dataclass
class _Failed:
    """The check that decided, and the live generation when it moved (check 3)."""

    reason: str
    live_generation: int | None = None


async def _incident_open(
    session: AsyncSession, nomination: RemediationNomination, window: int
) -> bool:
    """Whether an incident is open on the nomination's target key.

    @spec AUTOMATED-REMEDIATION-10 (maintainer ruling, 2026-10-07): a
    remediation's ledger record on this agent and target key (automatic or
    approved) whose verification has not finished, or finished less than the
    window ago. The window is the longest of the evaluating policy's and the
    executing policy's. Never read from an alert body.
    """

    executing = cast(
        RemediationPolicyGeneration.document["limits"]["incident_window_seconds"].astext,
        Integer,
    )
    seconds = func.greatest(INCIDENT_WINDOW_SECONDS_MINIMUM, window, executing)
    found = await session.scalar(
        select(AgentAction.id)
        .join(RemediationNomination, RemediationNomination.id == AgentAction.nomination_id)
        .outerjoin(
            RemediationPolicyGeneration,
            and_(
                RemediationPolicyGeneration.agent_id == RemediationNomination.agent_id,
                RemediationPolicyGeneration.hook == RemediationNomination.hook,
                RemediationPolicyGeneration.generation
                == func.coalesce(
                    RemediationNomination.current_generation,
                    RemediationNomination.admitted_generation,
                ),
            ),
        )
        .where(
            AgentAction.agent_id == nomination.agent_id,
            RemediationNomination.agent_id == nomination.agent_id,
            RemediationNomination.target == nomination.target,
            or_(
                AgentAction.verified_at.is_(None),
                AgentAction.verified_at + func.make_interval(0, 0, 0, 0, 0, 0, seconds)
                > func.now(),
            ),
        )
        .limit(1)
    )
    return found is not None


async def _limit_refusal(
    session: AsyncSession, nomination: RemediationNomination, document: Mapping[str, Any]
) -> str | None:
    """Check 11, counted under the admission lock. @spec AUTOMATED-REMEDIATION-10."""

    held = and_(
        RemediationReservation.agent_id == nomination.agent_id,
        RemediationReservation.released_at.is_(None),
        RemediationReservation.nomination_id != nomination.id,
    )
    turn = await session.scalar(
        select(RemediationReservation.nomination_id)
        .where(held, RemediationReservation.event_id == nomination.event_id)
        .limit(1)
    )
    if turn is not None:
        return TURN_LIMIT
    live = await session.scalar(
        select(RemediationReservation.nomination_id)
        .join(
            RemediationNomination,
            RemediationNomination.id == RemediationReservation.nomination_id,
        )
        .where(
            held,
            RemediationReservation.target == nomination.target,
            RemediationNomination.state.in_(LIVE_STATES),
        )
        .limit(1)
    )
    if live is not None:
        return TARGET_LIVE
    limits = _limits(document)
    window = _positive(limits.get("incident_window_seconds")) or INCIDENT_WINDOW_SECONDS_MINIMUM
    if await _incident_open(session, nomination, window):
        return INCIDENT_LIMIT
    hour = and_(
        held,
        RemediationReservation.hook == nomination.hook,
        RemediationReservation.reserved_at
        > func.now() - func.make_interval(0, 0, 0, 0, 0, 0, _HOUR_SECONDS),
    )
    per_action = _positive(limits.get("per_action_per_hour"))
    if per_action is not None:
        count = await session.scalar(
            select(func.count())
            .select_from(RemediationReservation)
            .where(hour, RemediationReservation.action == nomination.action)
        )
        if int(count or 0) >= per_action:
            return ACTION_RATE_LIMIT
    per_policy = min(
        _positive(limits.get("per_policy_per_hour")) or PER_POLICY_PER_HOUR_CEILING,
        PER_POLICY_PER_HOUR_CEILING,
    )
    count = await session.scalar(
        select(func.count()).select_from(RemediationReservation).where(hour)
    )
    if int(count or 0) >= per_policy:
        return POLICY_RATE_LIMIT
    return None


async def _reversible_now(
    session: AsyncSession, store: ObjectStore, agent_id: uuid.UUID, connector: str
) -> bool:
    """Check 9: the restore pair recorded at the in-force digest, and key custody.

    @spec AUTOMATED-REMEDIATION-8 (check 9), ACTION-EXECUTOR-13 and -16.
    """

    digest = await in_force_connector_digest(session, store, agent_id, connector)
    capable = await session.scalar(
        select(ConnectorCapability.restore_capable).where(
            ConnectorCapability.agent_id == agent_id,
            ConnectorCapability.connector == connector,
            ConnectorCapability.digest == (digest or ""),
        )
    )
    if digest is None or not capable:
        return False
    custody = await sealing_custody(session, store, [agent_id])
    return connector in custody.get(agent_id, frozenset())


async def _checks(
    session: AsyncSession,
    store: ObjectStore,
    nomination: RemediationNomination,
    live: RemediationPolicy | None,
    document: Mapping[str, Any],
    action: Mapping[str, Any],
) -> _Failed | None:
    """Checks 3 to 11, in order; the first that fails. The lock is held."""

    if (
        nomination.admitted_generation is None
        or live is None
        or live.generation != nomination.admitted_generation
    ):
        return _Failed(GENERATION_NOT_CURRENT, live.generation if live is not None else None)
    if not (live.armed and live.active):
        return _Failed(POLICY_DISARMED)
    if action.get("kind") != "remediate" or action.get("automatic") is not True:
        return _Failed(NOT_AUTOMATIC)
    qualification = await qualification_refusal(session, nomination.agent_id, action, store=store)
    if qualification is not None:
        return _Failed(
            qualification
            if qualification in (QUALIFICATION_MISSING, QUALIFICATION_STALE)
            else QUALIFICATION_MISSING
        )
    if await independence_refusal(session, nomination.agent_id, action, store=store):
        return _Failed("verifier_not_independent")
    try:
        arguments = json.loads(nomination.arguments or "")
    except ValueError:
        arguments = None
    if not isinstance(arguments, dict) or not within_bounds(action, arguments):
        return _Failed(OUT_OF_BOUNDS)
    connector, tool = action.get("connector"), action.get("tool")
    if not isinstance(connector, str) or not isinstance(tool, str) or nomination.target is None:
        return _Failed(OUT_OF_BOUNDS)
    if action.get("reversibility") == "reversible" and not await _reversible_now(
        session, store, nomination.agent_id, connector
    ):
        return _Failed(NOT_REVERSIBLE_NOW)
    if await breaker_open(session, nomination.agent_id, connector, tool, nomination.target):
        return _Failed(BREAKER_OPEN)
    limit = await _limit_refusal(session, nomination, document)
    if limit is not None:
        return _Failed(limit)
    return None


@dataclass
class _Context:
    """What a locked evaluation read: the nomination, its policy and action."""

    nomination: RemediationNomination
    live: RemediationPolicy | None
    document: Mapping[str, Any]
    action: Mapping[str, Any]


async def _locked_context(
    session: AsyncSession, nomination_id: uuid.UUID, expected: str
) -> _Context | _Failed | None:
    """Take the agent's admission lock and read the nomination and its policy.

    None when the nomination is gone or no longer in ``expected``. The admitted
    generation's document decides the action (immutable); when there is none,
    the current generation's, so check 3 still names the right reason.
    """

    nomination = await session.scalar(
        select(RemediationNomination)
        .where(RemediationNomination.id == nomination_id)
        .execution_options(populate_existing=True)
    )
    if nomination is None or nomination.state != expected:
        return None
    await session.execute(_ADMISSION_LOCK, {"agent": str(nomination.agent_id)})
    nomination = await session.scalar(
        select(RemediationNomination)
        .where(RemediationNomination.id == nomination_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if nomination is None or nomination.state != expected or nomination.action is None:
        return None
    live = await session.scalar(
        select(RemediationPolicy)
        .where(
            RemediationPolicy.agent_id == nomination.agent_id,
            RemediationPolicy.hook == nomination.hook,
        )
        .execution_options(populate_existing=True)
    )
    number = nomination.admitted_generation
    if number is None and live is not None:
        number = live.generation
    generation = (
        await session.scalar(
            select(RemediationPolicyGeneration).where(
                RemediationPolicyGeneration.agent_id == nomination.agent_id,
                RemediationPolicyGeneration.hook == nomination.hook,
                RemediationPolicyGeneration.generation == number,
            )
        )
        if number is not None
        else None
    )
    document = generation.document if generation is not None else None
    action = declared_action(document, nomination.action) if isinstance(document, Mapping) else None
    if not isinstance(document, Mapping) or action is None:
        # No generation declares the action the nomination was validated against.
        return _Failed(GENERATION_NOT_CURRENT, live.generation if live is not None else None)
    return _Context(nomination=nomination, live=live, document=document, action=action)


async def _reserve_and_read(
    session: AsyncSession, store: ObjectStore, context: _Context
) -> _Failed | None:
    """Write the reservation and the precondition read; ``precondition_pending``.

    @spec AUTOMATED-REMEDIATION-9 @spec AUTOMATED-REMEDIATION-10. In the
    transaction that holds the lock. A read that cannot be scheduled (no
    declared precondition, no in-force digest for its connector) is
    ``precondition_unavailable`` and reserves nothing.
    """

    nomination, action = context.nomination, context.action
    precondition = action.get("precondition")
    if not isinstance(precondition, Mapping) or nomination.admitted_generation is None:
        return _Failed(PRECONDITION_UNAVAILABLE)
    connector, tool = precondition.get("connector"), precondition.get("tool")
    arguments = precondition.get("arguments", {})
    pointer = precondition.get("pointer")
    if (
        not isinstance(connector, str)
        or not isinstance(tool, str)
        or not isinstance(arguments, Mapping)
        or not isinstance(pointer, str)
    ):
        return _Failed(PRECONDITION_UNAVAILABLE)
    digest = await in_force_connector_digest(session, store, nomination.agent_id, connector)
    if digest is None:
        return _Failed(PRECONDITION_UNAVAILABLE)
    now = await session.scalar(select(func.now()))
    if not isinstance(now, datetime):
        return _Failed(PRECONDITION_UNAVAILABLE)
    ref = policy_ref(
        nomination.agent_id, nomination.hook, nomination.admitted_generation, nomination.id
    )
    try:
        row = scheduled_read(
            agent_id=nomination.agent_id,
            connector=connector,
            connector_digest=digest,
            tool=tool,
            arguments=arguments,
            pointer=pointer,
            authority_kind=POLICY_AUTHORITY,
            authority_ref=f"{ref}:{_PRECONDITION}",
            idempotency_key=precondition_key(nomination.id),
            not_before=now,
        )
    except ReadRefused:
        return _Failed(PRECONDITION_UNAVAILABLE)
    await session.execute(
        insert(ActionExecution)
        .values(row)
        .on_conflict_do_nothing(constraint="uq_action_executions_agent_idempotency_key")
    )
    await session.execute(
        insert(RemediationReservation)
        .values(
            nomination_id=nomination.id,
            agent_id=nomination.agent_id,
            hook=nomination.hook,
            action=nomination.action,
            target=nomination.target,
            event_id=nomination.event_id,
        )
        .on_conflict_do_nothing(index_elements=["nomination_id"])
    )
    nomination.state = PRECONDITION_PENDING
    return None


# --------------------------------------------------------------------------- #
# Leaving admission: approval or a stopped agent
# --------------------------------------------------------------------------- #


async def _delivery_turn(
    session: AsyncSession, nomination: RemediationNomination
) -> QueuedTurn | None:
    """The protected delivery's turn as an approval request needs it, or None.

    @spec AUTOMATED-REMEDIATION-15: ``reply_kind`` and ``reply_channel`` (and
    the nullable reply columns) are copied from the protected delivery's
    ``QueuedTurn``, never its text. The submission row holds them: the
    conversation from the binding's ``logical_conversation_key`` and the reply
    surface the hook route recorded for the delivery
    (``remediation_delivery_surfaces``), copied at nomination time, so the
    surface is the one the delivery selected even after the agent's channels
    change. None when no reply surface was recorded: there is nowhere to ask,
    and nothing is guessed.
    """

    submission = await session.scalar(
        select(RemediationNominationSubmission).where(
            RemediationNominationSubmission.event_id == nomination.event_id
        )
    )
    if submission is None or not submission.reply_kind or not submission.reply_channel:
        return None
    return QueuedTurn(
        event_id=nomination.event_id,
        conversation_id=submission.conversation_id
        or hook_conversation_id(nomination.agent_id, nomination.hook),
        author=f"hook:{nomination.hook}",
        text="",
        source=TurnSource.WEBHOOK,
        tool_access=ToolAccess.READ_ONLY,
        reply_handle=ReplyHandle(
            kind=submission.reply_kind,
            channel=submission.reply_channel,
            placeholder=None,
            endpoint=submission.reply_endpoint,
            adapter=submission.reply_adapter,
        ),
        received_at=datetime.now(UTC).isoformat(),
    )


async def raise_approval(
    session: AsyncSession, nomination_id: uuid.UUID, *, observed: Any = None
) -> uuid.UUID | None:
    """Raise (or attach to) the approval of a nomination already sent to approval.

    @spec AUTOMATED-REMEDIATION-15. The nomination is ``approval_requested``
    with its ``approval_reason``; this names the approval on it, on the reply
    surface its delivery recorded. A nomination whose delivery recorded none,
    and any other failure, is left for ``reconcile_admissions``, which ends an
    unraisable one ``refused`` (``reply_surface_unavailable``). Commits;
    returns the approval id.
    """

    try:
        nomination = await session.get(RemediationNomination, nomination_id, populate_existing=True)
        if (
            nomination is None
            or nomination.state != APPROVAL_REQUESTED
            or nomination.approval_reason is None
        ):
            await session.rollback()
            return None
        if nomination.approval_id is not None:
            approval_id = nomination.approval_id
            await session.rollback()
            return approval_id
        turn = await _delivery_turn(session, nomination)
        if turn is None:
            # Nowhere to ask: the reconciler's next pass ends it
            # ``reply_surface_unavailable`` (``_end_unraisable``).
            await session.rollback()
            logger.warning("remediation approval has no reply surface nomination=%s", nomination_id)
            return None
        requested = await request_remediation_approval(
            session,
            nomination_id,
            turn=turn,
            check=nomination.approval_reason,
            observed=observed,
        )
    except (RemediationApprovalUnavailable, SQLAlchemyError, ValueError) as error:
        await session.rollback()
        logger.warning(
            "remediation approval not raised nomination=%s error=%s",
            nomination_id,
            type(error).__name__,
        )
        return None
    return requested.approval_id


async def return_to_approval(
    session: AsyncSession,
    nomination_id: uuid.UUID,
    reason: str,
    *,
    from_states: Iterable[str],
    live_generation: int | None = None,
) -> bool:
    """Send the nomination to approval naming ``reason`` and release its reservation.

    @spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-10. Only from
    ``from_states``; commits nothing. Returns whether it moved. ``raise_approval``
    names the approval once the caller commits.
    """

    values: dict[str, Any] = {
        "state": APPROVAL_REQUESTED,
        "approval_reason": reason,
        "decided_at": func.now(),
    }
    if live_generation is not None:
        # Both generations are recorded (AUTOMATED-REMEDIATION-4).
        values["current_generation"] = live_generation
    moved = await session.scalar(
        update(RemediationNomination)
        .where(
            RemediationNomination.id == nomination_id,
            RemediationNomination.state.in_(list(from_states)),
        )
        .values(**values)
        .returning(RemediationNomination.id)
        .execution_options(synchronize_session=False)
    )
    await release_reservation(session, nomination_id)
    return moved is not None


async def _to_approval(
    session: AsyncSession,
    nomination_id: uuid.UUID,
    failed: _Failed,
    *,
    from_state: str,
    observed: Any = None,
) -> None:
    await session.rollback()
    moved = await return_to_approval(
        session,
        nomination_id,
        failed.reason,
        from_states=(from_state,),
        live_generation=failed.live_generation,
    )
    await session.commit()
    if moved:
        logger.info(
            "remediation nomination sent to approval nomination=%s reason=%s",
            nomination_id,
            failed.reason,
        )
        await raise_approval(session, nomination_id, observed=observed)


async def _stop(session: AsyncSession, nomination_id: uuid.UUID, *, from_state: str) -> None:
    """``refused`` ``agent_stopped``: a stopped agent asks nobody (check 2)."""

    await session.rollback()
    await session.execute(
        update(RemediationNomination)
        .where(RemediationNomination.id == nomination_id, RemediationNomination.state == from_state)
        .values(state=REFUSED, refusal_code=AGENT_STOPPED, decided_at=func.now())
        .execution_options(synchronize_session=False)
    )
    await release_reservation(session, nomination_id)
    await session.commit()
    logger.info(
        "remediation nomination refused nomination=%s code=%s", nomination_id, AGENT_STOPPED
    )


# --------------------------------------------------------------------------- #
# Admission at submission
# --------------------------------------------------------------------------- #


async def _admit_one(
    session: AsyncSession, store: ObjectStore, kill_switch: KillSwitch, nomination_id: uuid.UUID
) -> None:
    current = await session.scalar(
        select(RemediationNomination.agent_id).where(
            RemediationNomination.id == nomination_id,
            RemediationNomination.state == RECEIVED,
        )
    )
    await session.rollback()
    if current is None:
        return
    if await _stopped(kill_switch, current):
        await _stop(session, nomination_id, from_state=RECEIVED)
        return
    failed: _Failed | None
    try:
        context = await _locked_context(session, nomination_id, RECEIVED)
        if context is None:
            await session.rollback()
            return
        if isinstance(context, _Failed):
            failed = context
        else:
            failed = await _checks(
                session, store, context.nomination, context.live, context.document, context.action
            )
            if failed is None:
                failed = await _reserve_and_read(session, store, context)
        if failed is None:
            await session.commit()
            logger.info(
                "remediation nomination admitted to its precondition read %s", nomination_id
            )
            return
    except SQLAlchemyError:
        failed = _Failed(ADMISSION_UNREADABLE)
    await _to_approval(session, nomination_id, failed, from_state=RECEIVED)


async def admit_nominations(
    session: AsyncSession,
    store: ObjectStore,
    kill_switch: KillSwitch,
    nomination_ids: Sequence[uuid.UUID],
) -> None:
    """Admit each ``received`` nomination of a submission, in block order.

    @spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-9 @spec AUTOMATED-REMEDIATION-10.
    A nomination already decided (a replayed submission) is left as it is.
    Each nomination is decided in its own transactions, so a later entry of the
    same turn sees the first one's reservation (``turn_limit``).
    """

    for nomination_id in nomination_ids:
        await _admit_one(session, store, kill_switch, nomination_id)


# --------------------------------------------------------------------------- #
# Check 12 and the re-check at precondition_pending -> admitted
# --------------------------------------------------------------------------- #


async def _recheck_and_admit(
    session: AsyncSession, store: ObjectStore, nomination_id: uuid.UUID, sample: Mapping[str, Any]
) -> _Failed | None:
    """Evaluate the precondition, re-run checks 3 to 11, admit and create the forward.

    @spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-9. One
    transaction under the lock; the forward execution is created in it.
    """

    context = await _locked_context(session, nomination_id, PRECONDITION_PENDING)
    if context is None:
        await session.rollback()
        return None
    if isinstance(context, _Failed):
        return context
    precondition = context.action.get("precondition")
    verdict = (
        evaluate_sample(precondition, sample) if isinstance(precondition, Mapping) else UNSUCCESSFUL
    )
    if verdict == UNSUCCESSFUL:
        return _Failed(PRECONDITION_UNAVAILABLE)
    if verdict != SATISFIED:
        return _Failed(PRECONDITION_NOT_MET)
    failed = await _checks(
        session, store, context.nomination, context.live, context.document, context.action
    )
    if failed is not None:
        return failed
    context.nomination.state = ADMITTED
    context.nomination.decided_at = func.now()
    await session.flush()
    try:
        # Commits the admission, the reservation and the execution together.
        await create_remediation_forward(session, nomination_id, store=store)
    except ForwardRefused:
        return _Failed(ADMISSION_UNREADABLE)
    logger.info("remediation nomination admitted nomination=%s", nomination_id)
    return None


async def precondition_ended(
    session: AsyncSession,
    store: ObjectStore,
    kill_switch: KillSwitch,
    read_id: uuid.UUID,
) -> None:
    """Decide check 12 for the nomination whose precondition read ended.

    @spec AUTOMATED-REMEDIATION-8 @spec AUTOMATED-REMEDIATION-9. Called after
    the transition that ended the read committed; a read of any other producer,
    or a nomination no longer ``precondition_pending``, is ignored. Commits.
    """

    read = await session.get(ActionExecution, read_id, populate_existing=True)
    nomination_id = precondition_nomination(read.idempotency_key) if read is not None else None
    if read is None or nomination_id is None or read.kind != ExecutionKind.read:
        await session.rollback()
        return
    state, sample = read.state, dict(read.sample or {})
    nomination = await session.get(RemediationNomination, nomination_id, populate_existing=True)
    agent_id = nomination.agent_id if nomination is not None else None
    pending = nomination is not None and nomination.state == PRECONDITION_PENDING
    await session.rollback()
    if not pending or agent_id is None:
        return
    if state not in (
        ExecutionState.confirmed,
        ExecutionState.refused,
        ExecutionState.failed,
        ExecutionState.indeterminate,
    ):
        return
    if await _stopped(kill_switch, agent_id):
        await _stop(session, nomination_id, from_state=PRECONDITION_PENDING)
        return
    observed = sample.get("value") if sample.get("sample") == "value" else None
    if state != ExecutionState.confirmed:
        await _to_approval(
            session,
            nomination_id,
            _Failed(PRECONDITION_UNAVAILABLE),
            from_state=PRECONDITION_PENDING,
        )
        return
    try:
        failed = await _recheck_and_admit(session, store, nomination_id, sample)
    except SQLAlchemyError:
        failed = _Failed(ADMISSION_UNREADABLE)
    if failed is not None:
        await _to_approval(
            session, nomination_id, failed, from_state=PRECONDITION_PENDING, observed=observed
        )


async def preconditions_ended(
    session: AsyncSession,
    store: ObjectStore,
    kill_switch: KillSwitch,
    executions: Iterable[ActionExecution | uuid.UUID],
) -> None:
    """``precondition_ended`` for each precondition read among ``executions``."""

    for item in executions:
        read_id = item if isinstance(item, uuid.UUID) else item.id
        await precondition_ended(session, store, kill_switch, read_id)


# --------------------------------------------------------------------------- #
# E8: the claim-time remediation authority hook
# --------------------------------------------------------------------------- #


async def authority_refusal(session: AsyncSession, execution: ActionExecution) -> str | None:
    """``policy_changed`` when a policy forward execution's authority no longer holds.

    @spec AUTOMATED-REMEDIATION-11 (executor amendment E8). For a ``policy``
    forward execution only: its nomination's admitted generation is still the
    hook's current generation and armed, and no breaker is open for its
    connector, tool and target key; otherwise ``policy_changed``. A policy
    execution no remediation nomination names (created through the seam by
    another producer) is not judged here. Reads only, in the caller's
    transaction.
    """

    if execution.kind != ExecutionKind.forward or execution.authority_kind != POLICY_AUTHORITY:
        return None
    nomination = await nomination_for_execution(session, execution)
    if nomination is None:
        # No nomination names it: not a remediation's execution, so the
        # remediation authority has nothing to re-validate (E8 judges those).
        return None
    if nomination.target is None or execution.tool is None:
        return POLICY_CHANGED
    live = await session.scalar(
        select(RemediationPolicy)
        .where(
            RemediationPolicy.agent_id == nomination.agent_id,
            RemediationPolicy.hook == nomination.hook,
        )
        .execution_options(populate_existing=True)
    )
    if (
        live is None
        or live.generation != nomination.admitted_generation
        or not (live.armed and live.active)
    ):
        return POLICY_CHANGED
    if await breaker_open(
        session, execution.agent_id, execution.connector, execution.tool, nomination.target
    ):
        return POLICY_CHANGED
    return None


async def refuse_changed_authority(
    session: AsyncSession, execution: ActionExecution
) -> uuid.UUID | None:
    """Return a refused policy execution's nomination to approval ``policy_changed``.

    @spec AUTOMATED-REMEDIATION-11 (E8). The caller has ended the execution
    ``refused`` ``policy_changed``; in its transaction the nomination (still
    ``admitted``) goes to approval and its reservation is released. Returns the
    nomination to ``raise_approval`` for once the caller commits.
    """

    nomination = await nomination_for_execution(session, execution)
    if nomination is None:
        return None
    moved = await return_to_approval(
        session, nomination.id, POLICY_CHANGED, from_states=(ADMITTED,)
    )
    return nomination.id if moved else None


# --------------------------------------------------------------------------- #
# Reconciliation
# --------------------------------------------------------------------------- #


def _owed_approval() -> Any:
    """``approval_requested`` with its reason and no approval yet."""

    return and_(
        RemediationNomination.state == APPROVAL_REQUESTED,
        RemediationNomination.approval_id.is_(None),
        RemediationNomination.approval_reason.is_not(None),
    )


async def _end_unraisable(session: AsyncSession) -> None:
    """End every owed approval request that no pass could ever raise. Commits nothing."""

    surface = (
        select(RemediationNominationSubmission.event_id)
        .where(
            RemediationNominationSubmission.event_id == RemediationNomination.event_id,
            RemediationNominationSubmission.reply_kind.is_not(None),
            RemediationNominationSubmission.reply_channel.is_not(None),
        )
        .exists()
    )
    live = (
        select(RemediationPolicy.generation)
        .where(
            RemediationPolicy.agent_id == RemediationNomination.agent_id,
            RemediationPolicy.hook == RemediationNomination.hook,
        )
        .scalar_subquery()
    )
    declared = (
        select(RemediationPolicyGeneration.generation)
        .where(
            RemediationPolicyGeneration.agent_id == RemediationNomination.agent_id,
            RemediationPolicyGeneration.hook == RemediationNomination.hook,
            RemediationPolicyGeneration.generation
            == func.coalesce(RemediationNomination.current_generation, live),
            RemediationPolicyGeneration.document["actions"].contains(
                func.jsonb_build_array(
                    func.jsonb_build_object("name", RemediationNomination.action)
                )
            ),
        )
        .exists()
    )
    for condition, code in ((~surface, REPLY_SURFACE_UNAVAILABLE), (~declared, UNKNOWN_ACTION)):
        ended = (
            await session.scalars(
                update(RemediationNomination)
                .where(_owed_approval(), condition)
                .values(state=REFUSED, refusal_code=code, decided_at=func.now())
                .returning(RemediationNomination.id)
                .execution_options(synchronize_session=False)
            )
        ).all()
        for nomination_id in ended:
            await release_reservation(session, nomination_id)
        if ended:
            logger.warning("remediation nominations refused count=%d code=%s", len(ended), code)


async def reconcile_admissions(
    session: AsyncSession, store: ObjectStore, kill_switch: KillSwitch, *, limit: int = 100
) -> int:
    """Finish admission work a crash or an unreadable store left undone.

    @spec AUTOMATED-REMEDIATION-8. Admits a ``received`` nomination its
    submission left undecided, decides check 12 for a ``precondition_pending``
    nomination whose read already ended, and raises the approval of an
    ``approval_requested`` nomination that names its reason but no approval.
    Each step is the same idempotent step the routes run. Returns how many it
    handled; failures are logged and retried on the next pass.

    An owed approval that can never be raised is ended first, in one statement
    for every such row, so it is never selected again and cannot starve a
    raisable one: no reply surface recorded is ``reply_surface_unavailable``,
    and an action the generation the approval would bind no longer declares is
    ``unknown_action`` (AUTOMATED-REMEDIATION-7, -15). A recorded delivery reply
    surface is pruned once its submission copied it.
    """

    await _end_unraisable(session)
    # A delivery's surface is needed until its submission copies it; the
    # binding it shadows keeps no expiry, so an unsubmitted one stays too.
    await session.execute(
        delete(RemediationDeliverySurface).where(
            select(RemediationNominationSubmission.event_id)
            .where(RemediationNominationSubmission.event_id == RemediationDeliverySurface.event_id)
            .exists()
        )
    )
    await session.commit()
    stranded = (
        await session.scalars(
            select(RemediationNomination.id)
            .where(
                RemediationNomination.state == RECEIVED,
                RemediationNomination.created_at
                < func.now() - func.make_interval(0, 0, 0, 0, 0, 0, _STRANDED_SECONDS),
            )
            .order_by(RemediationNomination.created_at, RemediationNomination.id)
            .limit(limit)
        )
    ).all()
    ended_reads = (
        await session.scalars(
            select(ActionExecution.id)
            .join(
                RemediationNomination,
                ActionExecution.idempotency_key
                == func.concat(IDEMPOTENCY_PREFIX, RemediationNomination.id, f":{_PRECONDITION}"),
            )
            .where(
                RemediationNomination.state == PRECONDITION_PENDING,
                ActionExecution.agent_id == RemediationNomination.agent_id,
                ActionExecution.kind == ExecutionKind.read,
                ActionExecution.state.in_(
                    (
                        ExecutionState.confirmed,
                        ExecutionState.refused,
                        ExecutionState.failed,
                        ExecutionState.indeterminate,
                    )
                ),
            )
            .limit(limit)
        )
    ).all()
    owed = (
        await session.scalars(
            select(RemediationNomination.id)
            .where(_owed_approval())
            .order_by(RemediationNomination.decided_at)
            .limit(limit)
        )
    ).all()
    await session.commit()
    handled = 0
    for step, ids in (
        ("admit", stranded),
        ("precondition", ended_reads),
        ("approval", owed),
    ):
        for item in ids:
            try:
                if step == "admit":
                    await _admit_one(session, store, kill_switch, item)
                elif step == "precondition":
                    await precondition_ended(session, store, kill_switch, item)
                else:
                    await raise_approval(session, item)
                handled += 1
            except Exception:  # noqa: BLE001 - one nomination must not stop the pass
                await session.rollback()
                logger.exception("remediation admission reconciliation failed step=%s", step)
    return handled


__all__ = [
    "ADMISSION_UNREADABLE",
    "POLICY_CHANGED",
    "admit_nominations",
    "authority_refusal",
    "precondition_ended",
    "precondition_key",
    "precondition_nomination",
    "preconditions_ended",
    "qualification_refusal",
    "raise_approval",
    "reconcile_admissions",
    "refuse_changed_authority",
    "return_to_approval",
    "within_bounds",
]
