"""A factory revision run's learned rule proposal and its settlement (#5796, ADR 0287).

A revision run admitted from reviewer feedback may propose one rule through
``POST /work-items/rule-proposal``. The proposal is stored as a hidden
``candidate`` in the repository scope of the run's WorkItem. The agent, the
tenant, the scope, the source request, the source pull request lineage, the
source URL and the proposer are read here from the verified capability's
request and WorkItem rows, never from the model. The model supplies only the
rule sentence and why it is general, bounded by ``curie_internal.memory_rules``.

When the pull request reaches a terminal state, the writer of that state calls
``settle_lineage_rule_candidates`` in its own transaction: on ``merged`` the
newest candidate of the lineage becomes ``proposed`` with the next number in
its scope and every older one ``superseded``; on ``closed`` every candidate
becomes ``discarded``. Only ``candidate`` rows change, so a second pass over a
terminal lineage changes nothing.

Lock order, agent first, the order agent deletion takes (it locks the agent
row, then the WorkItems, and reaches requests, rules and lineages through its
deletes and cascades). A proposal takes the agent row (``FOR KEY SHARE``,
``lock_rule_owner``), then its request row (``FOR KEY SHARE``) and then its
lineage row (``FOR SHARE``) before it rechecks eligibility and inserts. A
terminal writer takes the agent row ``FOR KEY SHARE`` too (or, on the
publication advance, the WorkItem and request rows) before it updates the
lineage row, and only then settles, which locks the lineage's candidate rows
and never the agent row (``curie_internal.memory_rules``). A proposal
therefore either commits before the terminal write and is settled by it, or
waits for it and is refused as ineligible; and agent deletion either waits for
a proposal or settlement, or runs first and leaves it refused or with nothing
to settle.

A proposal is taken only from the live execution. After the recheck it asks
the issue read route's own fence (``issue_read.read_issue_authority`` with
``running``): the request is ``running`` with a runtime owner, its heartbeat
lease and its execution deadline are unexpired, and its WorkItem is not
cancelled. A finished revision's capability still verifies until it
expires, so without this fence a late call
could store a candidate newer than the live revision's and supersede it on
merge. The eligibility answer itself does not demand a running execution,
because the completion contract reads it at sandbox boot, before the start.

A database failure while storing is answered ``ProposalUnavailable``, which
carries no text: the engine displays bound parameters in its error messages,
so the SQLAlchemy error, which would carry the rule and the reason, is dropped
here and never reaches a log or a traceback.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Final

from curie_internal.memory_rules import (
    FORMAT_VERSION,
    InvalidRule,
    RuleState,
    repository_scope_key,
    settle_lineage_candidates,
    validate_reason,
    validate_rule_text,
)
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError, StatementError
from sqlalchemy.ext.asyncio import AsyncSession

from .issue_read import IssueReadRefused, read_issue_authority
from .models import (
    Agent,
    ExecutionRequest,
    MemoryRule,
    MemoryRuleEvent,
    ThreadPublicationLineage,
    WorkItem,
)

NOT_ELIGIBLE: Final = "rule_proposal_not_eligible"
ALREADY_PROPOSED: Final = "rule_already_proposed"
# The reply targets of a revision admitted from reviewer feedback
# (`github_factory_review.admit_parsed_feedback`).
_REVISION_TARGETS: Final = ("pull_request", "review_thread")
_SOURCE_REQUEST_KEY: Final = "memory_rules_source_request_key"


class ProposalRefused(Exception):
    """A refused proposal; ``code`` is the only thing the caller answers with."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ProposalUnavailable(Exception):
    """The database could not store the proposal; carries no text and no cause."""


@dataclass(frozen=True)
class Eligible:
    """The platform's values for one eligible request's candidate."""

    agent_id: uuid.UUID
    tenant_id: uuid.UUID
    request_id: uuid.UUID
    lineage_id: uuid.UUID
    scope_key: str
    scope_label: str
    source_url: str
    proposed_by: str


async def rule_proposal_eligibility(
    session: AsyncSession, *, request_id: uuid.UUID, work_item_id: uuid.UUID
) -> Eligible | None:
    """The candidate's values when the request may propose a rule, else None.

    Eligible exactly when the WorkItem's agent has ``memory_rule_proposals``
    on, the request is a revision admitted from reviewer feedback (reply
    target ``pull_request`` or ``review_thread`` with a URL) with a recorded
    requester, the WorkItem is bound to a publication lineage that is still
    ``open``, and the WorkItem is not cancelled. Reads only; fails closed on
    anything missing. It does not ask whether the execution is running: the
    completion contract reads it at sandbox boot, before the start, and
    ``propose_candidate`` adds that fence itself.
    """

    row = (
        await session.execute(
            select(
                Agent.id,
                Agent.tenant_id,
                Agent.memory_rule_proposals,
                ExecutionRequest.reply_target_kind,
                ExecutionRequest.reply_target_url,
                ExecutionRequest.requester,
                WorkItem.publication_lineage_id,
                WorkItem.cancelled_at,
                WorkItem.code_host_kind,
                WorkItem.code_host_host,
                WorkItem.repository_project_id,
                WorkItem.repository_path,
                ThreadPublicationLineage.status,
            )
            .select_from(ExecutionRequest)
            .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
            .join(Agent, Agent.id == WorkItem.agent_id)
            .outerjoin(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
            .where(ExecutionRequest.id == request_id, WorkItem.id == work_item_id)
        )
    ).one_or_none()
    if row is None:
        return None
    (
        agent_id,
        tenant_id,
        switch,
        target_kind,
        target_url,
        requester,
        lineage_id,
        cancelled_at,
        code_host_kind,
        code_host_host,
        project_id,
        repository_path,
        lineage_status,
    ) = row
    if (
        switch is not True
        or target_kind not in _REVISION_TARGETS
        or not target_url
        or not requester
        or lineage_id is None
        or lineage_status != "open"
        or cancelled_at is not None
    ):
        return None
    try:
        scope_key = repository_scope_key(code_host_kind, code_host_host, project_id)
    except InvalidRule:
        return None
    return Eligible(
        agent_id=agent_id,
        tenant_id=tenant_id,
        request_id=request_id,
        lineage_id=lineage_id,
        scope_key=scope_key,
        scope_label=repository_path,
        source_url=target_url,
        proposed_by=requester,
    )


def _constraint(exc: IntegrityError) -> str | None:
    return getattr(exc.orig.__cause__, "constraint_name", None) if exc.orig else None


async def propose_candidate(
    session: AsyncSession,
    *,
    request_id: uuid.UUID,
    work_item_id: uuid.UUID,
    rule: str,
    reason: str,
) -> None:
    """Store the request's one rule candidate and its ``propose`` event, committed.

    Raises ``ProposalRefused`` (``rule_proposal_not_eligible`` or
    ``rule_already_proposed``) or ``InvalidRule`` (``invalid_text``,
    ``too_large``, ``reserved_text``); every refusal rolls back and stores
    nothing. A request that is no longer the live execution (not ``running``,
    no runtime owner, an expired heartbeat lease or execution deadline, or a
    cancelled WorkItem) is ``rule_proposal_not_eligible`` (module docstring).
    A database failure rolls back as far as it can and raises
    ``ProposalUnavailable``. No exception carries the submitted text.
    """

    try:
        first = await rule_proposal_eligibility(
            session, request_id=request_id, work_item_id=work_item_id
        )
        if first is None:
            raise ProposalRefused(NOT_ELIGIBLE)
        text = validate_rule_text(rule)
        why = validate_reason(reason)
        # Agent first (module docstring): an agent deleted meanwhile is gone.
        if not await lock_rule_owner(session, first.agent_id):
            raise ProposalRefused(NOT_ELIGIBLE)
        await session.execute(
            select(ExecutionRequest.id)
            .where(ExecutionRequest.id == request_id)
            .with_for_update(read=True, key_share=True)
        )
        lineage_id = await session.scalar(
            select(WorkItem.publication_lineage_id).where(WorkItem.id == work_item_id)
        )
        if lineage_id is None:
            raise ProposalRefused(NOT_ELIGIBLE)
        await session.execute(
            select(ThreadPublicationLineage.id)
            .where(ThreadPublicationLineage.id == lineage_id)
            .with_for_update(read=True)
        )
        # Under the lineage lock: a terminal write either committed before it
        # (so the lineage reads terminal here) or waits for this commit and
        # then settles this candidate.
        eligible = await rule_proposal_eligibility(
            session, request_id=request_id, work_item_id=work_item_id
        )
        if (
            eligible is None
            or eligible.lineage_id != lineage_id
            or eligible.agent_id != first.agent_id
        ):
            raise ProposalRefused(NOT_ELIGIBLE)
        # Only the live execution proposes: a finished revision's capability
        # still verifies (module docstring).
        try:
            await read_issue_authority(session, execution_request_id=request_id, running=True)
        except IssueReadRefused:
            raise ProposalRefused(NOT_ELIGIBLE) from None
        if (
            await session.scalar(
                select(MemoryRule.id).where(MemoryRule.source_request_id == request_id)
            )
            is not None
        ):
            raise ProposalRefused(ALREADY_PROPOSED)
        state: RuleState = "candidate"
        rule_id = uuid.uuid4()
        session.add(
            MemoryRule(
                id=rule_id,
                tenant_id=eligible.tenant_id,
                agent_id=eligible.agent_id,
                scope_kind="repository",
                scope_key=eligible.scope_key,
                scope_label=eligible.scope_label,
                number=None,
                state=state,
                text=text,
                sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                format_version=FORMAT_VERSION,
                reason=why,
                proposed_by=eligible.proposed_by,
                proposed_at=func.clock_timestamp(),
                source_request_id=request_id,
                source_lineage_id=eligible.lineage_id,
                source_url=eligible.source_url,
            )
        )
        await session.flush()
        session.add(
            MemoryRuleEvent(
                rule_id=rule_id,
                tenant_id=eligible.tenant_id,
                agent_id=eligible.agent_id,
                event="propose",
                from_state=None,
                to_state=state,
                actor_kind="agent",
                actor=str(eligible.agent_id),
            )
        )
        await session.commit()
    except IntegrityError as exc:
        await _rollback_quietly(session)
        if _constraint(exc) == _SOURCE_REQUEST_KEY:
            raise ProposalRefused(ALREADY_PROPOSED) from None
        # Its message displays the bound rule and reason: never re-raised.
        raise ProposalUnavailable from None
    except StatementError:
        await _rollback_quietly(session)
        raise ProposalUnavailable from None
    except (ProposalRefused, InvalidRule):
        await session.rollback()
        raise


async def _rollback_quietly(session: AsyncSession) -> None:
    """Roll back after a database failure; a dead connection may refuse even that."""

    try:
        await session.rollback()
    except (SQLAlchemyError, OSError):
        pass


async def lock_rule_owner(session: AsyncSession, agent_id: uuid.UUID) -> bool:
    """Hold the agent row ``FOR KEY SHARE``; False when the agent is gone.

    Every transaction that proposes or settles a learned rule takes this first
    (module docstring), so agent deletion, which locks the agent row ``FOR
    UPDATE`` before anything else, serializes with it instead of deadlocking.
    """

    return (
        await session.scalar(
            select(Agent.id).where(Agent.id == agent_id).with_for_update(read=True, key_share=True)
        )
    ) is not None


async def settle_lineage_rule_candidates(
    session: AsyncSession,
    lineage_id: uuid.UUID,
    terminal_state: str,
) -> int:
    """Promote, supersede or discard the lineage's candidates (ADR 0287 decision 5).

    Runs inside the caller's transaction, after its terminal lineage write and
    before its commit; it never commits or rolls back. The one implementation
    is ``curie_internal.memory_rules.settle_lineage_candidates``, which the
    worker's terminal lineage write calls too. Returns how many candidates
    changed.
    """

    return await settle_lineage_candidates(
        session, lineage_id=lineage_id, terminal_state=terminal_state
    )
