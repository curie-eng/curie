"""Running a remediation's verifier: scheduled samples, four outcomes, independence.

@spec AUTOMATED-REMEDIATION-17 @spec AUTOMATED-REMEDIATION-18 @spec AUTOMATED-REMEDIATION-19

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the maintainer
rulings of 2026-10-07 (M2, M3) and executor amendments E3, E5 and E9.

Scheduling (``schedule_verification``, called by
``POST /action-executions/{id}/outcome`` when a remediation's forward execution
ends). On ``confirmed``, every sample is created at once in the outcome's
transaction (the claim route's skip rule needs the successor rows): one ``read``
execution per sample of the declared verifier read, due every
``interval_seconds`` after the forward's ``dispatched_at`` up to and including
``deadline_seconds`` (at most 60), under the forward's authority. When the
acting connector's digest is recorded ``restore_capable`` (it advertises
``observe_version``) and the ledger record carries its target and
``post_version``, an observe-only execution of the acting connector is due
beside each sample (E3). The nomination is ``verifying``. A forward that ends
``failed``, ``indeterminate`` or ``refused`` after admission
(``finish_unverified``) gets no verifier: its nomination finishes
``not-recovered`` at once with the execution's code in ``execution_code``, and
a ledger record, when one exists, carries the outcome too. A refusal that
returns the nomination to approval (``not_reversible_now``, ``policy_changed``)
is not an outcome. A verifier that cannot be scheduled (none declared, no in-force digest
for its connector, not independent now) is ``verifier-unavailable``.

Evaluation (``reads_ended``, called after every transition that ends a read):

* ``superseded`` first: any later ledger record on the same target key (a
  remediation's, by its nomination's key; a model turn's or an approval's, by
  its connector and the value of the action's declared target argument in its
  arguments), or an observe-only execution that reported a version other than
  the record's ``post_version``. Attribution, never success;
* ``verifier-unavailable`` at once when a sample read was ``refused``;
* ``verified`` once ``consecutive`` adjacent samples due at or after settle
  are satisfied, and every observe-only execution due by then has ended (so a
  version change seen beside the deciding sample still supersedes);
* after the last sample and observation end: ``not-recovered`` when a sample
  at or after settle was successful, else ``verifier-unavailable``.

Samples due before settle are taken and recorded but never count. The outcome
is written once, on the ledger record (``verification_outcome`` with
``verified_at``) and the nomination (``verification_outcome``, ``finished``);
the rest of the verification's reads are then ended ``refused``
``authority_unavailable`` (the authority that scheduled them is spent) and are
never claimed. No outcome creates a restore execution or calls the undo
ruling (AUTOMATED-REMEDIATION-19). Nothing here logs or returns a sampled
value, a version or a target.

Independence (``independence_refusal``): the verifier's connector differs from
the acting connector and, in the agent's in-force version, the secret names
their MCP headers expand are disjoint; otherwise ``verifier_not_independent``.
Checked at policy write, at scheduling and, through this function, at
admission.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Final

from plugin_format.connectors import ConnectorSpec
from sqlalchemy import cast, literal, or_, select, update
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from . import bundles
from .action_execution_codes import NOT_REVERSIBLE_NOW_CODE
from .action_undoable import in_force_bundle_refs
from .config import get_settings
from .models import (
    ActionExecution,
    AgentAction,
    ConnectorCapability,
    ExecutionKind,
    ExecutionState,
    RemediationNomination,
)
from .remediation_forward import (
    IDEMPOTENCY_PREFIX,
    declared_action,
    in_force_connector_digest,
    nomination_for_execution,
    policy_generation,
)
from .remediation_predicate import SATISFIED, SUCCESSFUL_SAMPLES, evaluate_sample
from .remediation_reads import OBSERVE_TOOL, ReadRefused, scheduled_read
from .storage import BundleStore, ObjectStore

logger = logging.getLogger(__name__)

NOT_INDEPENDENT: Final = "verifier_not_independent"
VERIFIED: Final = "verified"
NOT_RECOVERED: Final = "not-recovered"
UNAVAILABLE: Final = "verifier-unavailable"
SUPERSEDED: Final = "superseded"

VERIFYING: Final = "verifying"
FINISHED: Final = "finished"

# @spec AUTOMATED-REMEDIATION-12: a verification is at most 60 samples.
MAX_SAMPLES: Final = 60
# @spec AUTOMATED-REMEDIATION-11 @spec AUTOMATED-REMEDIATION-13: refusals that send
# the nomination back to approval instead of finishing it.
_BACK_TO_APPROVAL: Final = frozenset({NOT_REVERSIBLE_NOW_CODE, "policy_changed"})
_UNVERIFIED: Final = frozenset(
    {ExecutionState.failed, ExecutionState.indeterminate, ExecutionState.refused}
)

# The code the rest of a decided verification's reads end with: the authority
# that scheduled them is spent, and no read under it runs.
SPENT_CODE: Final = "authority_unavailable"

_VERIFY: Final = "verify"
_SAMPLE: Final = "sample"
_OBSERVE: Final = "observe"
_TERMINAL: Final = frozenset(
    {
        ExecutionState.confirmed,
        ExecutionState.failed,
        ExecutionState.indeterminate,
        ExecutionState.refused,
    }
)


# --------------------------------------------------------------------------- #
# Keys
# --------------------------------------------------------------------------- #


def _verification_prefix(nomination_id: uuid.UUID, forward_id: uuid.UUID) -> str:
    """``remediation:<nomination id>:verify:<forward execution id>:``."""

    return f"{IDEMPOTENCY_PREFIX}{nomination_id}:{_VERIFY}:{forward_id}:"


def _read_key(nomination_id: uuid.UUID, forward_id: uuid.UUID, role: str, index: int) -> str:
    return f"{_verification_prefix(nomination_id, forward_id)}{role}:{index}"


def _parse_read_key(key: str | None) -> tuple[uuid.UUID, uuid.UUID] | None:
    """``(nomination id, forward execution id)`` of a verification read's key."""

    if not key or not key.startswith(IDEMPOTENCY_PREFIX):
        return None
    parts = key.removeprefix(IDEMPOTENCY_PREFIX).split(":")
    if len(parts) != 5 or parts[1] != _VERIFY or parts[3] not in (_SAMPLE, _OBSERVE):
        return None
    try:
        return uuid.UUID(parts[0]), uuid.UUID(parts[2])
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Independence (AUTOMATED-REMEDIATION-17)
# --------------------------------------------------------------------------- #

# ``$NAME``, ``${NAME}`` and ``${NAME:-default}``: what an MCP client expands
# from the sandbox environment in a remote connector's headers.
_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::?-[^}]*)?\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def header_secret_names(spec: ConnectorSpec) -> frozenset[str]:
    """The secret names a connector's MCP headers expand.

    A hosted connector's header is derived (``connector_render``):
    ``Authorization: Bearer ${NAME}`` from ``bearer_secret``, otherwise from a
    lone plain string secret; a ``SecretRef`` alone derives none. A remote
    connector's headers name theirs as environment references.
    """

    if spec.is_hosted:
        if spec.bearer_secret:
            return frozenset({spec.bearer_secret})
        if len(spec.secrets) == 1 and isinstance(spec.secrets[0], str):
            return frozenset({spec.secrets[0]})
        return frozenset()
    names: set[str] = set()
    for value in spec.headers.values():
        for braced, bare in _REFERENCE.findall(value):
            names.add(braced or bare)
    return frozenset(names)


def _connectors_of(data: bytes) -> dict[str, ConnectorSpec]:
    settings = get_settings()
    with TemporaryDirectory() as tmp:
        bundles.extract_stored_bundle(
            data,
            Path(tmp),
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        return dict(bundles.read_connectors(Path(tmp)).connectors)


class _Unreadable(Exception):
    """The in-force version exists but its connectors cannot be read."""


async def _in_force_connectors(
    session: AsyncSession, store: ObjectStore, agent_id: uuid.UUID
) -> dict[str, ConnectorSpec] | None:
    """The in-force version's connectors; None when no version is in force."""

    bundle_ref = (await in_force_bundle_refs(session, [agent_id])).get(agent_id)
    if bundle_ref is None:
        return None
    try:
        data = await store.get(bundle_ref)
        return await run_in_threadpool(_connectors_of, data)
    except Exception as exc:  # noqa: BLE001 - an unreadable version proves nothing
        raise _Unreadable(type(exc).__name__) from None


async def independence_refusal(
    session: AsyncSession,
    agent_id: uuid.UUID,
    action: Mapping[str, Any],
    *,
    store: ObjectStore | None = None,
) -> str | None:
    """``verifier_not_independent`` or None, judged against the in-force version now.

    @spec AUTOMATED-REMEDIATION-17 (and AUTOMATED-REMEDIATION-8 check 7). The
    verifier's connector must differ from the acting one, and the secret names
    their MCP headers expand must be disjoint. With no version in force only the
    names are compared; a version in force that cannot be read fails closed.
    Residual trust, named by the spec: disjoint names do not prove distinct
    credentials.
    """

    verifier = action.get("verifier")
    if not isinstance(verifier, Mapping):
        return None
    acting, reading = action.get("connector"), verifier.get("connector")
    if not isinstance(acting, str) or not isinstance(reading, str) or acting == reading:
        return NOT_INDEPENDENT
    try:
        connectors = await _in_force_connectors(
            session, store or BundleStore(get_settings()), agent_id
        )
    except _Unreadable:
        return NOT_INDEPENDENT
    if connectors is None:
        return None
    acting_spec, reading_spec = connectors.get(acting), connectors.get(reading)
    if acting_spec is None or reading_spec is None:
        return None
    if header_secret_names(acting_spec) & header_secret_names(reading_spec):
        return NOT_INDEPENDENT
    return None


# --------------------------------------------------------------------------- #
# Writing the outcome once
# --------------------------------------------------------------------------- #


async def _end_the_rest(
    session: AsyncSession, nomination_id: uuid.UUID, forward_id: uuid.UUID, now: datetime
) -> None:
    """End every read of the verification still waiting: it is never claimed.

    A row a concurrent claim holds is skipped here; that claim's own
    evaluation finds the outcome written and ends it then.
    """

    prefix = _verification_prefix(nomination_id, forward_id)
    waiting = (
        await session.scalars(
            select(ActionExecution.id)
            .where(
                ActionExecution.kind == ExecutionKind.read,
                ActionExecution.state == ExecutionState.requested,
                ActionExecution.idempotency_key.startswith(prefix, autoescape=True),
            )
            .with_for_update(skip_locked=True)
        )
    ).all()
    if not waiting:
        return
    await session.execute(
        update(ActionExecution)
        .where(
            ActionExecution.id.in_(waiting),
            ActionExecution.state == ExecutionState.requested,
        )
        .values(
            state=ExecutionState.refused.value,
            refusal_code=SPENT_CODE,
            finished_at=now,
        )
        .execution_options(synchronize_session=False)
    )


async def _write_outcome(
    session: AsyncSession,
    *,
    action_id: uuid.UUID,
    nomination_id: uuid.UUID,
    forward_id: uuid.UUID,
    outcome: str,
    now: datetime,
) -> None:
    """@spec AUTOMATED-REMEDIATION-18: the one outcome, on the record and the nomination.

    Each update holds only while no outcome is written, so a second outcome
    changes nothing. No restore and no undo follow (AUTOMATED-REMEDIATION-19).
    """

    written = await session.scalar(
        update(AgentAction)
        .where(AgentAction.id == action_id, AgentAction.verification_outcome.is_(None))
        .values(verification_outcome=outcome, verified_at=now)
        .returning(AgentAction.id)
        .execution_options(synchronize_session=False)
    )
    if written is None:
        return
    await session.execute(
        update(RemediationNomination)
        .where(
            RemediationNomination.id == nomination_id,
            RemediationNomination.verification_outcome.is_(None),
        )
        .values(verification_outcome=outcome, state=FINISHED)
        .execution_options(synchronize_session=False)
    )
    await _end_the_rest(session, nomination_id, forward_id, now)
    logger.info("remediation verification ended nomination=%s outcome=%s", nomination_id, outcome)


# --------------------------------------------------------------------------- #
# Scheduling (AUTOMATED-REMEDIATION-18)
# --------------------------------------------------------------------------- #


def _seconds(declaration: Mapping[str, Any], key: str) -> int | None:
    value = declaration.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


async def _verifier_of(
    session: AsyncSession, execution: ActionExecution, nomination: RemediationNomination
) -> tuple[Mapping[str, Any], Mapping[str, Any]] | None:
    """The declared action and its verifier, from the generation the authority names."""

    if nomination.action is None:
        return None
    generation = await policy_generation(session, nomination, execution.authority_kind)
    declared = (
        declared_action(generation.document, nomination.action) if generation is not None else None
    )
    verifier = declared.get("verifier") if declared is not None else None
    if declared is None or not isinstance(verifier, Mapping):
        return None
    return declared, verifier


async def _observable(
    session: AsyncSession, execution: ActionExecution, action: AgentAction
) -> bool:
    """@spec AUTOMATED-REMEDIATION-18 (E3): the acting connector advertises
    ``observe_version`` at this digest and the record names what to observe.
    """

    if not isinstance(action.target, dict) or not action.post_version:
        return False
    capable = await session.scalar(
        select(ConnectorCapability.restore_capable).where(
            ConnectorCapability.agent_id == execution.agent_id,
            ConnectorCapability.connector == execution.connector,
            ConnectorCapability.digest == execution.connector_digest,
        )
    )
    return bool(capable)


async def finish_unverified(
    session: AsyncSession, execution: ActionExecution, now: datetime
) -> bool:
    """Finish a remediation whose forward execution ended without confirming.

    @spec AUTOMATED-REMEDIATION-18: "An execution that ends ``failed``,
    ``indeterminate`` or ``refused`` after admission gets no verifier and
    finishes ``not-recovered`` for reporting, with the execution code". The
    nomination is ``finished`` ``not-recovered`` with ``execution_code``; a
    ledger record (``failed`` and ``indeterminate`` have one, a refusal never
    does) carries the outcome too. Written once. A refusal that returns the
    nomination to approval is left alone. Returns whether ``execution`` was such
    an ending of a remediation's forward execution. Commits nothing.
    """

    if execution.kind != ExecutionKind.forward or execution.state not in _UNVERIFIED:
        return False
    code = execution.refusal_code or execution.failure_code
    if execution.state == ExecutionState.refused and code in _BACK_TO_APPROVAL:
        return True
    nomination = await nomination_for_execution(session, execution)
    if nomination is None:
        return True
    if execution.subject_action_id is not None:
        await session.execute(
            update(AgentAction)
            .where(
                AgentAction.id == execution.subject_action_id,
                AgentAction.verification_outcome.is_(None),
            )
            .values(verification_outcome=NOT_RECOVERED, verified_at=now)
            .execution_options(synchronize_session=False)
        )
    finished = await session.scalar(
        update(RemediationNomination)
        .where(
            RemediationNomination.id == nomination.id,
            RemediationNomination.verification_outcome.is_(None),
        )
        .values(verification_outcome=NOT_RECOVERED, state=FINISHED, execution_code=code)
        .returning(RemediationNomination.id)
        .execution_options(synchronize_session=False)
    )
    if finished is not None:
        logger.info(
            "remediation verification ended nomination=%s outcome=%s", nomination.id, NOT_RECOVERED
        )
    return True


async def schedule_verification(
    session: AsyncSession,
    store: ObjectStore,
    execution: ActionExecution,
    now: datetime,
) -> None:
    """Start, or decide at once, the verification of a remediation's forward execution.

    @spec AUTOMATED-REMEDIATION-18. Called in the transaction that ends
    ``execution``; writes nothing for a forward execution no nomination names.
    Commits nothing.
    """

    if await finish_unverified(session, execution, now):
        return
    if execution.kind != ExecutionKind.forward or execution.subject_action_id is None:
        return
    nomination = await nomination_for_execution(session, execution)
    if nomination is None:
        return
    action = await session.scalar(
        select(AgentAction)
        .where(AgentAction.id == execution.subject_action_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if action is None or action.verification_outcome is not None:
        return

    async def decide(outcome: str) -> None:
        await _write_outcome(
            session,
            action_id=action.id,
            nomination_id=nomination.id,
            forward_id=execution.id,
            outcome=outcome,
            now=now,
        )

    if execution.state != ExecutionState.confirmed:
        return

    found = await _verifier_of(session, execution, nomination)
    if found is None:
        await decide(UNAVAILABLE)
        return
    declared, verifier = found
    interval = _seconds(verifier, "interval_seconds")
    deadline = _seconds(verifier, "deadline_seconds")
    connector, tool = verifier.get("connector"), verifier.get("tool")
    arguments, pointer = verifier.get("arguments"), verifier.get("pointer")
    if (
        not interval
        or deadline is None
        or execution.dispatched_at is None
        or not isinstance(connector, str)
        or not isinstance(tool, str)
        or not isinstance(arguments, Mapping)
        or not isinstance(pointer, str)
    ):
        await decide(UNAVAILABLE)
        return
    if await independence_refusal(session, execution.agent_id, declared, store=store):
        await decide(UNAVAILABLE)
        return
    digest = await in_force_connector_digest(session, store, execution.agent_id, connector)
    if digest is None:
        await decide(UNAVAILABLE)
        return

    observe = await _observable(session, execution, action)
    dispatched_at = execution.dispatched_at
    rows: list[dict[str, Any]] = []

    def scheduled(
        index: int,
        role: str,
        *,
        connector: str,
        digest: str,
        tool: str,
        arguments: Any,
        pointer: str | None,
    ) -> dict[str, Any]:
        row = scheduled_read(
            agent_id=execution.agent_id,
            connector=connector,
            connector_digest=digest,
            tool=tool,
            arguments=arguments,
            pointer=pointer,
            authority_kind=execution.authority_kind,
            authority_ref=execution.authority_ref,
            idempotency_key=_read_key(nomination.id, execution.id, role, index),
            not_before=dispatched_at + timedelta(seconds=index * interval),
        )
        # One clock for the whole series, and the sample due with an
        # observation is handed out before it, deterministically.
        row["created_at"] = now if role == _SAMPLE else now + timedelta(microseconds=1)
        return row

    try:
        for index in range(1, min(deadline // interval, MAX_SAMPLES) + 1):
            rows.append(
                scheduled(
                    index,
                    _SAMPLE,
                    connector=connector,
                    digest=digest,
                    tool=tool,
                    arguments=arguments,
                    pointer=pointer,
                )
            )
            if observe:
                rows.append(
                    scheduled(
                        index,
                        _OBSERVE,
                        connector=execution.connector,
                        digest=execution.connector_digest,
                        tool=OBSERVE_TOOL,
                        arguments={"target": action.target},
                        pointer=None,
                    )
                )
    except ReadRefused:
        await decide(UNAVAILABLE)
        return
    if not rows:
        await decide(UNAVAILABLE)
        return
    await session.execute(
        insert(ActionExecution)
        .values(rows)
        .on_conflict_do_nothing(constraint="uq_action_executions_agent_idempotency_key")
    )
    await session.execute(
        update(RemediationNomination)
        .where(
            RemediationNomination.id == nomination.id,
            RemediationNomination.verification_outcome.is_(None),
            RemediationNomination.state != FINISHED,
        )
        .values(state=VERIFYING)
        .execution_options(synchronize_session=False)
    )


# --------------------------------------------------------------------------- #
# Evaluation (AUTOMATED-REMEDIATION-18)
# --------------------------------------------------------------------------- #


def _record_target_matches(declared: Mapping[str, Any], action: AgentAction) -> Any | None:
    """A ledger record without a nomination on this verification's target key.

    The AUTOMATED-REMEDIATION-10 key of a record no nomination names: its
    connector (the ``connector`` column, else the ``mcp__<connector>__`` tool
    prefix) and the value of the declared action's target argument in its
    arguments, compared as JSON.
    """

    target = declared.get("target")
    connector = declared.get("connector")
    if not isinstance(target, Mapping) or not isinstance(connector, str):
        return None
    argument = target.get("argument")
    arguments = action.arguments or {}
    if not isinstance(argument, str) or argument not in arguments:
        return None
    connector_matches = or_(
        AgentAction.connector == connector,
        AgentAction.connector.is_(None)
        & AgentAction.tool.startswith(f"mcp__{connector}__", autoescape=True),
    )
    value = cast(literal(json.dumps(arguments[argument])), JSONB)
    return (
        AgentAction.nomination_id.is_(None)
        & connector_matches
        & (AgentAction.arguments[argument] == value)
    )


async def _superseded(
    session: AsyncSession,
    action: AgentAction,
    nomination: RemediationNomination,
    declared: Mapping[str, Any],
    observes: list[ActionExecution],
) -> bool:
    """Any later ledger record on the same target key, or another version observed."""

    for observed in observes:
        if observed.state != ExecutionState.confirmed:
            continue
        version = (observed.outcome or {}).get("observed_version")
        if version != action.post_version:
            return True
    same_target = []
    if nomination.target is not None:
        same_target.append(
            AgentAction.nomination_id.in_(
                select(RemediationNomination.id).where(
                    RemediationNomination.agent_id == action.agent_id,
                    RemediationNomination.target == nomination.target,
                )
            )
        )
    unnominated = _record_target_matches(declared, action)
    if unnominated is not None:
        same_target.append(unnominated)
    if not same_target:
        return False
    later = await session.scalar(
        select(AgentAction.id)
        .where(
            AgentAction.agent_id == action.agent_id,
            AgentAction.id != action.id,
            AgentAction.created_at > action.created_at,
            or_(*same_target),
        )
        .limit(1)
    )
    return later is not None


def _consecutive(verifier: Mapping[str, Any]) -> int:
    value = verifier.get("consecutive", 1)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return 1
    return value


async def _evaluate(
    session: AsyncSession, nomination_id: uuid.UUID, forward_id: uuid.UUID, now: datetime
) -> None:
    forward = await session.get(ActionExecution, forward_id)
    if forward is None or forward.subject_action_id is None or forward.dispatched_at is None:
        return
    action = await session.scalar(
        select(AgentAction)
        .where(AgentAction.id == forward.subject_action_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if action is None:
        return
    if action.verification_outcome is not None:
        await _end_the_rest(session, nomination_id, forward_id, now)
        return
    nomination = await session.get(RemediationNomination, nomination_id)
    if nomination is None or nomination.agent_id != forward.agent_id:
        return
    found = await _verifier_of(session, forward, nomination)
    if found is None:
        return
    declared, verifier = found
    settle = _seconds(verifier, "settle_seconds") or 0

    reads = (
        await session.scalars(
            select(ActionExecution)
            .where(
                ActionExecution.agent_id == forward.agent_id,
                ActionExecution.kind == ExecutionKind.read,
                ActionExecution.idempotency_key.startswith(
                    _verification_prefix(nomination_id, forward_id), autoescape=True
                ),
            )
            .order_by(ActionExecution.not_before, ActionExecution.created_at)
            .execution_options(populate_existing=True)
        )
    ).all()
    samples = [r for r in reads if f":{_SAMPLE}:" in r.idempotency_key]
    observes = [r for r in reads if f":{_OBSERVE}:" in r.idempotency_key]
    if not samples:
        return

    async def decide(outcome: str) -> None:
        await _write_outcome(
            session,
            action_id=action.id,
            nomination_id=nomination_id,
            forward_id=forward_id,
            outcome=outcome,
            now=now,
        )

    if await _superseded(session, action, nomination, declared, observes):
        await decide(SUPERSEDED)
        return
    if any(sample.state == ExecutionState.refused for sample in samples):
        await decide(UNAVAILABLE)
        return

    settle_at = forward.dispatched_at + timedelta(seconds=settle)
    counted = [s for s in samples if s.not_before is not None and s.not_before >= settle_at]
    needed = _consecutive(verifier)
    run = 0
    for sample in counted:
        if sample.state not in _TERMINAL:
            break
        if evaluate_sample(verifier, sample.sample or {}) == SATISFIED:
            run += 1
        else:
            run = 0
        if run >= needed:
            due = sample.not_before
            if any(
                o.state not in _TERMINAL
                for o in observes
                if o.not_before is not None and due is not None and o.not_before <= due
            ):
                # An observation due with the deciding sample may still supersede.
                return
            await decide(VERIFIED)
            return

    if all(r.state in _TERMINAL for r in reads):
        successful = any(
            (s.sample or {}).get("sample") in SUCCESSFUL_SAMPLES
            for s in counted
            if s.state == ExecutionState.confirmed
        )
        await decide(NOT_RECOVERED if successful else UNAVAILABLE)


async def reads_ended(session: AsyncSession, keys: Iterable[str | None], now: datetime) -> None:
    """Evaluate each verification one of these ended reads belongs to.

    @spec AUTOMATED-REMEDIATION-18. ``keys`` are the ended reads' idempotency
    keys; a read of any other producer is ignored. Locks each verification's
    ledger record, in a stable order, and commits nothing.
    """

    verifications = sorted(
        {parsed for key in keys if (parsed := _parse_read_key(key)) is not None},
        key=lambda pair: (str(pair[1]), str(pair[0])),
    )
    for nomination_id, forward_id in verifications:
        await _evaluate(session, nomination_id, forward_id, now)


def is_observe_only(execution: ActionExecution) -> bool:
    """@spec AUTOMATED-REMEDIATION-18 (E3): a read of ``observe_version`` with no pointer."""

    return (
        execution.kind == ExecutionKind.read
        and execution.tool == OBSERVE_TOOL
        and execution.pointer is None
    )


__all__ = [
    "MAX_SAMPLES",
    "NOT_INDEPENDENT",
    "NOT_RECOVERED",
    "SPENT_CODE",
    "SUPERSEDED",
    "UNAVAILABLE",
    "VERIFIED",
    "finish_unverified",
    "header_secret_names",
    "independence_refusal",
    "is_observe_only",
    "reads_ended",
    "schedule_verification",
]
