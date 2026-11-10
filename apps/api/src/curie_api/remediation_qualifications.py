"""Qualification records and qualification verifier runs (AUTOMATED-REMEDIATION-22, -23).

@spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-23

docs/superpowers/specs/2026-10-07-automated-remediation.md, with the
2026-10-07 principal ruling: a record enables automatic execution, so writing
one and starting a verifier run both need an ADR 0106 operator principal (the
routes in ``routers.remediation_qualifications`` take it).

A record (``record_qualification``) is written for one action declaration of a
policy generation: the generation fixes the connector, tool, reversibility,
verifier declaration and the bounds it was evaluated against, and the connector
digest is the acting connector's digest in the agent's in-force version at
write. Its evidence references rows of this agent, checked by kind, state and
digest when the record is written:

* reversible: ``restore_execution_id``, a ``confirmed`` restore of a ledger
  record this connector and tool produced at the digest, and
  ``conflict_execution_id``, a restore ``refused`` ``version_conflict`` with its
  ``refused_conflict`` audit row naming both versions (AE-15);
* idempotent: ``forward_execution_ids``, two distinct ``confirmed`` forwards
  authorized by approvals (a drill runs through an ordinary remediation
  approval) with the same canonical arguments whose records left the same
  ``post_version``;
* every action: ``verified_run_id`` and ``not_recovered_run_id``, verifier runs
  of this qualification under the same hook, action and verifier declaration
  that ended ``verified`` and ``not-recovered``.

Refusals are ``QualificationRefused`` with a named code and write nothing. A
record is immutable: a replay of the same body answers it, another body under
the same id is ``qualification_conflict``.

A verifier run (``start_verifier_run``) takes only a hook, an action and a
literal member of the action's ``target.allowed``; it schedules the declared
verifier's samples as AUTOMATED-REMEDIATION-18 does for a forward, anchored on
the run's ``started_at``: one ``read`` execution per interval to the deadline,
``authority_kind`` ``qualification``, ``authority_ref`` the run id, at the read
connector's in-force digest. ``qualification_reads_ended`` evaluates it
(``verified``, ``not-recovered``, ``verifier-unavailable``) once per ended
sample and ends the remaining samples once decided. No run opens a breaker,
touches a nomination or creates any execution but those reads: no code path
here lets an administrator request a write.

``qualification_refusal`` is admission check 6's record lookup and the policy
write's ``qualification_required`` check: ``qualification_missing`` without a
record of this agent for this declaration, ``qualification_stale`` when the
acting connector's in-force digest is no longer the record's.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .action_forward import arguments_sha256
from .config import get_settings
from .models import (
    ActionAuditEntry,
    ActionExecution,
    AgentAction,
    ExecutionKind,
    ExecutionState,
    RemediationPolicy,
    RemediationPolicyGeneration,
    RemediationQualification,
    RemediationQualificationVerifierRun,
)
from .remediation_forward import declared_action, in_force_connector_digest
from .remediation_predicate import SATISFIED, SUCCESSFUL_SAMPLES, evaluate_sample
from .remediation_reads import ReadRefused, scheduled_read
from .remediation_verifier import (
    MAX_SAMPLES,
    NOT_RECOVERED,
    SPENT_CODE,
    UNAVAILABLE,
    VERIFIED,
    independence_refusal,
)
from .storage import BundleStore, ObjectStore

logger = logging.getLogger(__name__)

QUALIFICATION_MISSING: Final = "qualification_missing"
QUALIFICATION_STALE: Final = "qualification_stale"
QUALIFICATION_REQUIRED: Final = "qualification_required"

# Refusal codes of the record write and the verifier-run route.
EVIDENCE_INCOMPLETE: Final = "evidence_incomplete"
EVIDENCE_NOT_FOUND: Final = "evidence_not_found"
EVIDENCE_WRONG_STATE: Final = "evidence_wrong_state"
EVIDENCE_OTHER_DIGEST: Final = "evidence_other_digest"
EVIDENCE_OTHER_ACTION: Final = "evidence_other_action"
EVIDENCE_NOT_IDEMPOTENT: Final = "evidence_not_idempotent"
DOCUMENT_INVALID: Final = "qualification_document_invalid"
CONFLICT: Final = "qualification_conflict"
UNKNOWN_ACTION: Final = "unknown_action"
TARGET_NOT_ALLOWED: Final = "target_not_allowed"
NOT_FOUND: Final = "qualification_not_found"

AUTHORITY_KIND: Final = "qualification"
WORST_CASE_MAX: Final = 2000

_RESTORE: Final = "restore_execution_id"
_CONFLICT: Final = "conflict_execution_id"
_FORWARDS: Final = "forward_execution_ids"
_VERIFIED_RUN: Final = "verified_run_id"
_NOT_RECOVERED_RUN: Final = "not_recovered_run_id"
_EVIDENCE_KEYS: Final = (_RESTORE, _CONFLICT, _FORWARDS, _VERIFIED_RUN, _NOT_RECOVERED_RUN)
_REQUIRED: Final[Mapping[str, tuple[str, ...]]] = {
    "reversible": (_RESTORE, _CONFLICT, _VERIFIED_RUN, _NOT_RECOVERED_RUN),
    "idempotent": (_FORWARDS, _VERIFIED_RUN, _NOT_RECOVERED_RUN),
}

# ``qualification:<run id>:sample:<index>``: the key of one run's sample.
KEY_PREFIX: Final = "qualification:"
_SAMPLE: Final = "sample"
_TERMINAL: Final = frozenset(
    {
        ExecutionState.confirmed,
        ExecutionState.failed,
        ExecutionState.indeterminate,
        ExecutionState.refused,
    }
)


class QualificationRefused(Exception):
    """A refusal that wrote nothing: a named code, a message and the HTTP status."""

    def __init__(self, code: str, message: str, *, status_code: int = 422) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.status_code = status_code


def verifier_sha256(verifier: Mapping[str, Any]) -> str:
    """The verifier declaration digest: SHA-256 over its canonical JSON."""

    return arguments_sha256(verifier)


def _uuid(value: Any) -> uuid.UUID | None:
    if not isinstance(value, str):
        return None
    try:
        return uuid.UUID(value)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# The record lookup (admission check 6, the policy write)
# --------------------------------------------------------------------------- #


async def qualification_refusal(
    session: AsyncSession,
    agent_id: uuid.UUID,
    action: Mapping[str, Any],
    *,
    store: ObjectStore | None = None,
) -> str | None:
    """``qualification_missing``, ``qualification_stale`` or None.

    @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-8 (check 6). The
    action's ``qualification`` reference must name a record of this agent for
    the same connector, tool, reversibility and verifier declaration, else
    ``qualification_missing``; the acting connector's digest in the in-force
    version must be the record's, else ``qualification_stale`` (no digest in
    force is stale too: fails closed). Reads only.
    """

    record_id = _uuid(action.get("qualification"))
    if record_id is None:
        return QUALIFICATION_MISSING
    record = await session.scalar(
        select(RemediationQualification)
        .where(
            RemediationQualification.id == record_id,
            RemediationQualification.agent_id == agent_id,
        )
        .execution_options(populate_existing=True)
    )
    verifier = action.get("verifier")
    if record is None or not isinstance(verifier, Mapping):
        return QUALIFICATION_MISSING
    try:
        declared_verifier = verifier_sha256(verifier)
    except (TypeError, ValueError):
        return QUALIFICATION_MISSING
    if (
        record.connector != action.get("connector")
        or record.tool != action.get("tool")
        or record.reversibility != action.get("reversibility")
        or record.verifier_sha256 != declared_verifier
    ):
        return QUALIFICATION_MISSING
    digest = await in_force_connector_digest(
        session, store or BundleStore(get_settings()), agent_id, record.connector
    )
    if digest != record.connector_digest:
        return QUALIFICATION_STALE
    return None


async def automatic_unqualified(
    session: AsyncSession,
    agent_id: uuid.UUID,
    document: Mapping[str, Any],
    *,
    store: ObjectStore | None = None,
) -> int | None:
    """The index of the first ``automatic`` action without a valid record, or None.

    @spec AUTOMATED-REMEDIATION-23: the policy write's ``qualification_required``
    check. Reads only.
    """

    actions = document.get("actions")
    for index, action in enumerate(actions if isinstance(actions, list) else ()):
        if not isinstance(action, Mapping) or action.get("automatic") is not True:
            continue
        if await qualification_refusal(session, agent_id, action, store=store) is not None:
            return index
    return None


# --------------------------------------------------------------------------- #
# The record write (AUTOMATED-REMEDIATION-22)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _Declaration:
    """What one record qualifies, read from the policy generation it names."""

    connector: str
    tool: str
    reversibility: str
    verifier_sha256: str


async def _declaration(
    session: AsyncSession, agent_id: uuid.UUID, hook: str, generation: int, name: str
) -> _Declaration:
    row = await session.get(RemediationPolicyGeneration, (agent_id, hook, generation))
    declared = declared_action(row.document, name) if row is not None else None
    if declared is None:
        raise QualificationRefused(UNKNOWN_ACTION, "that policy generation declares no such action")
    connector, tool = declared.get("connector"), declared.get("tool")
    reversibility, verifier = declared.get("reversibility"), declared.get("verifier")
    if (
        not isinstance(connector, str)
        or not isinstance(tool, str)
        or reversibility not in _REQUIRED
        or not isinstance(verifier, Mapping)
    ):
        raise QualificationRefused(
            DOCUMENT_INVALID,
            "only a reversible or idempotent action with a verifier can be qualified",
        )
    return _Declaration(connector, tool, str(reversibility), verifier_sha256(verifier))


def _references(reversibility: str, evidence: Mapping[str, Any]) -> dict[str, Any]:
    """The evidence references this reversibility requires, each well formed."""

    references: dict[str, Any] = {}
    for key in _REQUIRED[reversibility]:
        value = evidence.get(key)
        if key == _FORWARDS:
            if (
                not isinstance(value, list)
                or len(value) != 2
                or not all(isinstance(item, str) for item in value)
            ):
                raise QualificationRefused(
                    EVIDENCE_INCOMPLETE, f"{key} must name exactly two forward executions"
                )
        elif not isinstance(value, str):
            raise QualificationRefused(EVIDENCE_INCOMPLETE, f"{key} is required")
        references[key] = value
    return references


def _not_found(key: str) -> QualificationRefused:
    return QualificationRefused(EVIDENCE_NOT_FOUND, f"{key} names no row of this agent")


def _wrong_state(key: str, reason: str) -> QualificationRefused:
    return QualificationRefused(EVIDENCE_WRONG_STATE, f"{key}: {reason}")


def _other_action(key: str) -> QualificationRefused:
    return QualificationRefused(
        EVIDENCE_OTHER_ACTION, f"{key} is evidence of another connector, tool or verifier"
    )


def _other_digest(key: str) -> QualificationRefused:
    return QualificationRefused(
        EVIDENCE_OTHER_DIGEST, f"{key} ran at another digest than the one in force"
    )


def _tool_matches(record: AgentAction, connector: str, tool: str) -> bool:
    """The ledger record's tool, bare or as the model's ``mcp__<connector>__<tool>``."""

    return record.tool in (tool, f"mcp__{connector}__{tool}") and record.connector in (
        connector,
        None,
    )


class _Evidence:
    """Checks each reference against this agent's rows. Reads only."""

    def __init__(
        self,
        session: AsyncSession,
        agent_id: uuid.UUID,
        qualification_id: uuid.UUID,
        hook: str,
        action: str,
        declaration: _Declaration,
        digest: str,
    ) -> None:
        self.session = session
        self.agent_id = agent_id
        self.qualification_id = qualification_id
        self.hook = hook
        self.action = action
        self.declaration = declaration
        self.digest = digest

    async def _execution(self, key: str, raw: str) -> tuple[ActionExecution, AgentAction | None]:
        execution_id = _uuid(raw)
        execution = (
            await self.session.get(ActionExecution, execution_id)
            if execution_id is not None
            else None
        )
        if execution is None or execution.agent_id != self.agent_id:
            raise _not_found(key)
        record = (
            await self.session.get(AgentAction, execution.subject_action_id)
            if execution.subject_action_id is not None
            else None
        )
        if record is not None and record.agent_id != self.agent_id:
            record = None
        return execution, record

    def _same_action(self, key: str, execution: ActionExecution, record: AgentAction) -> None:
        if execution.connector != self.declaration.connector or not _tool_matches(
            record, self.declaration.connector, self.declaration.tool
        ):
            raise _other_action(key)
        if execution.connector_digest != self.digest or record.connector_digest != self.digest:
            raise _other_digest(key)

    async def restore(self, key: str, raw: str) -> None:
        """A ``confirmed`` restore of a record this tool produced at the digest."""

        execution, record = await self._execution(key, raw)
        if execution.kind != ExecutionKind.restore or execution.state != ExecutionState.confirmed:
            raise _wrong_state(key, "not a confirmed restore")
        if record is None:
            raise _wrong_state(key, "restores no ledger record")
        self._same_action(key, execution, record)

    async def conflict(self, key: str, raw: str) -> None:
        """A restore refused ``version_conflict`` with its audit row naming both versions."""

        execution, record = await self._execution(key, raw)
        if (
            execution.kind != ExecutionKind.restore
            or execution.state != ExecutionState.refused
            or execution.refusal_code != "version_conflict"
        ):
            raise _wrong_state(key, "not a restore refused version_conflict")
        if record is None:
            raise _wrong_state(key, "restores no ledger record")
        audits = await self.session.scalars(
            select(ActionAuditEntry.evidence).where(
                ActionAuditEntry.action_id == record.id,
                ActionAuditEntry.action == "refused_conflict",
            )
        )
        if not any(
            isinstance(evidence, Mapping)
            and evidence.get("recorded_version") is not None
            and evidence.get("observed_version") is not None
            for evidence in audits
        ):
            raise _wrong_state(key, "no refused_conflict audit row names both versions")
        self._same_action(key, execution, record)

    async def forwards(self, key: str, raws: Sequence[str]) -> None:
        """Two confirmed approval forwards whose repeat had no additional effect."""

        checked: list[tuple[ActionExecution, AgentAction]] = []
        for raw in raws:
            execution, record = await self._execution(key, raw)
            if (
                execution.kind != ExecutionKind.forward
                or execution.state != ExecutionState.confirmed
                or execution.authority_kind != "approval"
            ):
                raise _wrong_state(key, "not a confirmed forward an approval authorized")
            if record is None:
                raise _wrong_state(key, "left no ledger record")
            if execution.tool != self.declaration.tool:
                raise _other_action(key)
            self._same_action(key, execution, record)
            checked.append((execution, record))
        (first, first_record), (second, second_record) = checked
        if (
            first.id == second.id
            or first.arguments_sha256 is None
            or first.arguments_sha256 != second.arguments_sha256
            or not first_record.post_version
            or first_record.post_version != second_record.post_version
        ):
            raise QualificationRefused(
                EVIDENCE_NOT_IDEMPOTENT,
                "the repeat must be another forward with the same canonical arguments "
                "that left the same post_version",
            )

    async def run(self, key: str, raw: str, outcome: str) -> None:
        """A verifier run of this qualification under this declaration, ended ``outcome``."""

        run_id = _uuid(raw)
        run = (
            await self.session.get(RemediationQualificationVerifierRun, run_id)
            if run_id is not None
            else None
        )
        if (
            run is None
            or run.agent_id != self.agent_id
            or run.qualification_id != self.qualification_id
        ):
            raise _not_found(key)
        if run.outcome != outcome:
            raise _wrong_state(key, f"the run did not end {outcome}")
        if (
            run.hook != self.hook
            or run.action != self.action
            or run.verifier_sha256 != self.declaration.verifier_sha256
        ):
            raise _other_action(key)


def _stored_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    return {key: evidence[key] for key in _EVIDENCE_KEYS if key in evidence}


def _same_request(
    record: RemediationQualification,
    agent_id: uuid.UUID,
    hook: str,
    action: str,
    generation: int,
    evidence: Mapping[str, Any],
    worst_case: str,
) -> bool:
    return (
        record.agent_id == agent_id
        and record.hook == hook
        and record.action == action
        and record.generation == generation
        and record.worst_case == worst_case
        and record.evidence == _stored_evidence(evidence)
    )


async def record_qualification(
    session: AsyncSession,
    store: ObjectStore,
    *,
    agent_id: uuid.UUID,
    qualification_id: uuid.UUID,
    hook: str,
    action: str,
    generation: int,
    evidence: Mapping[str, Any],
    worst_case: str,
    principal: str,
) -> RemediationQualification:
    """Write one record after checking its evidence, or raise ``QualificationRefused``.

    @spec AUTOMATED-REMEDIATION-22. Commits the record; a refusal writes nothing.
    A replay of the same request answers the record already written.
    """

    def conflict() -> QualificationRefused:
        return QualificationRefused(
            CONFLICT, "this qualification id recorded another request", status_code=409
        )

    if not 1 <= len(worst_case) <= WORST_CASE_MAX or not worst_case.strip():
        raise QualificationRefused(
            DOCUMENT_INVALID, f"worst_case must be text of 1 to {WORST_CASE_MAX} characters"
        )
    existing = await session.get(RemediationQualification, qualification_id)
    if existing is not None:
        if _same_request(existing, agent_id, hook, action, generation, evidence, worst_case):
            return existing
        raise conflict()

    declaration = await _declaration(session, agent_id, hook, generation, action)
    references = _references(declaration.reversibility, evidence)
    digest = await in_force_connector_digest(session, store, agent_id, declaration.connector)
    if digest is None:
        raise QualificationRefused(
            EVIDENCE_OTHER_DIGEST, "the acting connector has no digest in the in-force version"
        )
    checks = _Evidence(session, agent_id, qualification_id, hook, action, declaration, digest)
    if declaration.reversibility == "reversible":
        await checks.restore(_RESTORE, references[_RESTORE])
        await checks.conflict(_CONFLICT, references[_CONFLICT])
    else:
        await checks.forwards(_FORWARDS, references[_FORWARDS])
    await checks.run(_VERIFIED_RUN, references[_VERIFIED_RUN], VERIFIED)
    await checks.run(_NOT_RECOVERED_RUN, references[_NOT_RECOVERED_RUN], NOT_RECOVERED)

    written = await session.scalar(
        insert(RemediationQualification)
        .values(
            id=qualification_id,
            agent_id=agent_id,
            connector=declaration.connector,
            tool=declaration.tool,
            connector_digest=digest,
            verifier_sha256=declaration.verifier_sha256,
            reversibility=declaration.reversibility,
            recorded_by=principal,
            worst_case=worst_case,
            evidence=_stored_evidence(evidence),
            hook=hook,
            action=action,
            generation=generation,
        )
        .on_conflict_do_nothing(index_elements=["id"])
        .returning(RemediationQualification.id)
    )
    await session.commit()
    record = await session.scalar(
        select(RemediationQualification)
        .where(RemediationQualification.id == qualification_id)
        .execution_options(populate_existing=True)
    )
    if record is None:  # pragma: no cover - the agent was deleted meanwhile
        raise QualificationRefused(NOT_FOUND, "the agent is gone", status_code=404)
    if written is None and not _same_request(
        record, agent_id, hook, action, generation, evidence, worst_case
    ):
        raise conflict()
    if written is not None:
        logger.info(
            "remediation qualification recorded agent=%s qualification=%s",
            agent_id,
            qualification_id,
        )
    return record


# --------------------------------------------------------------------------- #
# Verifier runs (AUTOMATED-REMEDIATION-22, scheduled as AUTOMATED-REMEDIATION-18)
# --------------------------------------------------------------------------- #


def _sample_key(run_id: uuid.UUID, index: int) -> str:
    return f"{KEY_PREFIX}{run_id}:{_SAMPLE}:{index}"


def _run_prefix(run_id: uuid.UUID) -> str:
    return f"{KEY_PREFIX}{run_id}:{_SAMPLE}:"


def _parse_key(key: str | None) -> uuid.UUID | None:
    if not key or not key.startswith(KEY_PREFIX):
        return None
    parts = key.removeprefix(KEY_PREFIX).split(":")
    if len(parts) != 3 or parts[1] != _SAMPLE:
        return None
    return _uuid(parts[0])


def _seconds(declaration: Mapping[str, Any], key: str) -> int | None:
    value = declaration.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _literal_member(target: Any, allowed: Any) -> bool:
    """``target`` is one of ``allowed`` by JSON type and exact value."""

    if not isinstance(allowed, list):
        return False
    return any(type(item) is type(target) and item == target for item in allowed)


async def _schedule(
    session: AsyncSession,
    store: ObjectStore,
    run: RemediationQualificationVerifierRun,
    declared: Mapping[str, Any],
    verifier: Mapping[str, Any],
) -> bool:
    """Insert the run's samples; False when the verifier cannot be scheduled."""

    interval = _seconds(verifier, "interval_seconds")
    deadline = _seconds(verifier, "deadline_seconds")
    connector, tool = verifier.get("connector"), verifier.get("tool")
    arguments, pointer = verifier.get("arguments"), verifier.get("pointer")
    if (
        not interval
        or deadline is None
        or not isinstance(connector, str)
        or not isinstance(tool, str)
        or not isinstance(arguments, Mapping)
        or not isinstance(pointer, str)
    ):
        return False
    if await independence_refusal(session, run.agent_id, declared, store=store):
        return False
    digest = await in_force_connector_digest(session, store, run.agent_id, connector)
    if digest is None:
        return False
    rows: list[dict[str, Any]] = []
    try:
        for index in range(1, min(deadline // interval, MAX_SAMPLES) + 1):
            row = scheduled_read(
                agent_id=run.agent_id,
                connector=connector,
                connector_digest=digest,
                tool=tool,
                arguments=arguments,
                pointer=pointer,
                authority_kind=AUTHORITY_KIND,
                authority_ref=str(run.id),
                idempotency_key=_sample_key(run.id, index),
                not_before=run.started_at + timedelta(seconds=index * interval),
            )
            row["created_at"] = run.started_at
            rows.append(row)
    except ReadRefused:
        return False
    if not rows:
        return False
    await session.execute(insert(ActionExecution).values(rows))
    return True


async def start_verifier_run(
    session: AsyncSession,
    store: ObjectStore,
    *,
    agent_id: uuid.UUID,
    qualification_id: uuid.UUID,
    hook: str,
    action: str,
    target: Any,
    principal: str,
) -> RemediationQualificationVerifierRun:
    """Start one verifier evaluation of the declared verifier, or raise.

    @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-18. The action is
    read from the hook's current generation; ``target`` must be a literal member
    of its ``target.allowed`` (``target_not_allowed``). Creates only ``read``
    executions with ``authority_kind`` ``qualification``. A verifier that cannot
    be scheduled (no in-force digest for its connector, not independent now) is
    ``verifier-unavailable`` at once. Commits.
    """

    current = await session.get(RemediationPolicy, (agent_id, hook))
    generation = (
        await session.get(RemediationPolicyGeneration, (agent_id, hook, current.generation))
        if current is not None and current.active
        else None
    )
    declared = declared_action(generation.document, action) if generation is not None else None
    verifier = declared.get("verifier") if declared is not None else None
    if generation is None or declared is None or not isinstance(verifier, Mapping):
        raise QualificationRefused(
            UNKNOWN_ACTION, "the hook's current policy declares no such action with a verifier"
        )
    declared_target = declared.get("target")
    allowed = declared_target.get("allowed") if isinstance(declared_target, Mapping) else None
    if not _literal_member(target, allowed):
        raise QualificationRefused(
            TARGET_NOT_ALLOWED, "the target must be a literal member of the action's allowed list"
        )
    now = (await session.execute(select(func.now()))).scalar_one()
    run = RemediationQualificationVerifierRun(
        id=uuid.uuid4(),
        agent_id=agent_id,
        qualification_id=qualification_id,
        hook=hook,
        action=action,
        generation=generation.generation,
        verifier_sha256=verifier_sha256(verifier),
        target=target,
        started_by=principal,
        started_at=now,
    )
    session.add(run)
    await session.flush()
    if not await _schedule(session, store, run, declared, verifier):
        run.outcome = UNAVAILABLE
        run.decided_at = now
    await session.commit()
    await session.refresh(run)
    logger.info("remediation qualification verifier run started agent=%s run=%s", agent_id, run.id)
    return run


async def read_verifier_run(
    session: AsyncSession, agent_id: uuid.UUID, qualification_id: uuid.UUID, run_id: uuid.UUID
) -> RemediationQualificationVerifierRun | None:
    """One run of this agent's qualification, or None. @spec AUTOMATED-REMEDIATION-22."""

    run: RemediationQualificationVerifierRun | None = await session.scalar(
        select(RemediationQualificationVerifierRun)
        .where(
            RemediationQualificationVerifierRun.id == run_id,
            RemediationQualificationVerifierRun.agent_id == agent_id,
            RemediationQualificationVerifierRun.qualification_id == qualification_id,
        )
        .execution_options(populate_existing=True)
    )
    return run


def _consecutive(verifier: Mapping[str, Any]) -> int:
    value = verifier.get("consecutive", 1)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return 1
    return value


def _decide(
    samples: Sequence[ActionExecution], verifier: Mapping[str, Any], anchor: datetime
) -> str | None:
    """@spec AUTOMATED-REMEDIATION-18's rules without ``superseded`` (no write to supersede)."""

    if any(sample.state == ExecutionState.refused for sample in samples):
        return UNAVAILABLE
    settle_at = anchor + timedelta(seconds=_seconds(verifier, "settle_seconds") or 0)
    counted = [s for s in samples if s.not_before is not None and s.not_before >= settle_at]
    needed, run = _consecutive(verifier), 0
    for sample in counted:
        if sample.state not in _TERMINAL:
            break
        run = run + 1 if evaluate_sample(verifier, sample.sample or {}) == SATISFIED else 0
        if run >= needed:
            return VERIFIED
    if samples and all(sample.state in _TERMINAL for sample in samples):
        successful = any(
            (s.sample or {}).get("sample") in SUCCESSFUL_SAMPLES
            for s in counted
            if s.state == ExecutionState.confirmed
        )
        return NOT_RECOVERED if successful else UNAVAILABLE
    return None


async def _end_the_rest(session: AsyncSession, run_id: uuid.UUID, now: datetime) -> None:
    """End every sample of a decided run still waiting: it is never claimed."""

    await session.execute(
        update(ActionExecution)
        .where(
            ActionExecution.kind == ExecutionKind.read,
            ActionExecution.state == ExecutionState.requested,
            ActionExecution.authority_kind == AUTHORITY_KIND,
            ActionExecution.idempotency_key.startswith(_run_prefix(run_id), autoescape=True),
        )
        .values(state=ExecutionState.refused.value, refusal_code=SPENT_CODE, finished_at=now)
        .execution_options(synchronize_session=False)
    )


async def _evaluate(session: AsyncSession, run_id: uuid.UUID, now: datetime) -> None:
    run = await session.scalar(
        select(RemediationQualificationVerifierRun)
        .where(RemediationQualificationVerifierRun.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None:
        return
    if run.outcome is not None:
        await _end_the_rest(session, run.id, now)
        return
    generation = await session.get(
        RemediationPolicyGeneration, (run.agent_id, run.hook, run.generation)
    )
    declared = declared_action(generation.document, run.action) if generation else None
    verifier = declared.get("verifier") if declared is not None else None
    if not isinstance(verifier, Mapping):
        return
    samples = (
        await session.scalars(
            select(ActionExecution)
            .where(
                ActionExecution.agent_id == run.agent_id,
                ActionExecution.kind == ExecutionKind.read,
                ActionExecution.authority_kind == AUTHORITY_KIND,
                ActionExecution.idempotency_key.startswith(_run_prefix(run.id), autoescape=True),
            )
            .order_by(ActionExecution.not_before, ActionExecution.created_at)
            .execution_options(populate_existing=True)
        )
    ).all()
    outcome = _decide(samples, verifier, run.started_at)
    if outcome is None:
        return
    run.outcome = outcome
    run.decided_at = now
    await session.flush()
    await _end_the_rest(session, run.id, now)
    logger.info("remediation qualification verifier run ended run=%s outcome=%s", run.id, outcome)


async def qualification_reads_ended(
    session: AsyncSession, keys: Iterable[str | None], now: datetime
) -> None:
    """Evaluate each qualification verifier run one of these ended reads belongs to.

    @spec AUTOMATED-REMEDIATION-22 @spec AUTOMATED-REMEDIATION-18. Called beside
    ``remediation_verifier.reads_ended`` after every transition that ends a
    read; a read of any other producer is ignored. Opens no breaker and touches
    no nomination. Commits nothing.
    """

    runs = sorted({run for key in keys if (run := _parse_key(key)) is not None}, key=str)
    for run_id in runs:
        await _evaluate(session, run_id, now)


__all__ = [
    "AUTHORITY_KIND",
    "QUALIFICATION_MISSING",
    "QUALIFICATION_REQUIRED",
    "QUALIFICATION_STALE",
    "QualificationRefused",
    "automatic_unqualified",
    "qualification_reads_ended",
    "qualification_refusal",
    "read_verifier_run",
    "record_qualification",
    "start_verifier_run",
    "verifier_sha256",
]
