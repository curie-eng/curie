"""Database access helpers for agents, versions, deployments, and approvals."""

import hashlib
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .models import (
    Agent,
    AgentChannel,
    AgentVersion,
    Approval,
    ApprovalAuditEntry,
    ApprovalStatus,
    ConsoleSession,
    DelegateGrant,
    DelegationCall,
    Deployment,
    Environment,
)
from .schemas import (
    AgentCreate,
    ApprovalRequest,
    ChannelBindingWrite,
    DelegateCallIn,
    DeploymentCreate,
    VersionCreate,
)


async def get_version(session: AsyncSession, version_id: uuid.UUID) -> AgentVersion | None:
    return await session.get(AgentVersion, version_id)


async def _refresh_with_channel(session: AsyncSession, agent: Agent) -> Agent:
    """Commit, then refresh the agent and name the `channel` relationship explicitly.

    The response model reads `agent.channel` after this session is done with, and
    an unloaded relationship RAISES under asyncio rather than lazy-loading, so
    the endpoint 500s where a crud-level test (holding a live session) passes.
    """
    await session.commit()
    await session.refresh(agent)
    await session.refresh(agent, ["channel"])
    return agent


async def attach_bundle(
    session: AsyncSession,
    version: AgentVersion,
    bundle_ref: str,
    bundle_sha256: str,
) -> AgentVersion:
    version.bundle_ref = bundle_ref
    version.bundle_sha256 = bundle_sha256
    await session.commit()
    await session.refresh(version)
    return version


async def create_agent(session: AsyncSession, data: AgentCreate) -> Agent:
    agent = Agent(
        name=data.name,
        # Attached through the relationship rather than inserted separately, so
        # the agent row and its binding are one transaction: a unique-constraint
        # collision on either rolls BOTH back, and no agent is ever left behind
        # bound to nothing (#38's silent-shadow state).
        # `endpoint`/`adapter` are the server-controlled reply route (ADR-0096
        # phase 2): both NULL for `slack` and for a binding whose route is
        # configured later, both set together otherwise. The write schema has
        # already refused a half-configured pair, and
        # `agent_channels_route_pair_ck` refuses one from an out-of-band writer.
        channel=AgentChannel(
            kind=data.channel.kind,
            address=data.channel.address,
            endpoint=data.channel.endpoint,
            adapter=data.channel.adapter,
        ),
        repo_full_name=data.repo_full_name,
        model=data.model,
        thinking=data.thinking,
        behavior_packs=(
            data.behavior_packs.model_dump() if data.behavior_packs is not None else None
        ),
        approval_required_tools=data.approval_required_tools,
        approval_routes=(
            {name: b.model_dump() for name, b in data.approval_routes.items()}
            if data.approval_routes is not None
            else None
        ),
        secrets=data.secrets,
    )
    session.add(agent)
    return await _refresh_with_channel(session, agent)


async def list_agents(session: AsyncSession) -> list[Agent]:
    # `selectinload` explicitly, even though the relationship is already
    # lazy="selectin": the list path is the one where the per-row alternative is
    # correct AND unboundedly slow, so a hundred-agent install would pay a
    # hundred round trips to render one page.
    result = await session.scalars(
        select(Agent).options(selectinload(Agent.channel)).order_by(Agent.created_at)
    )
    return list(result)


async def get_agent(session: AsyncSession, agent_id: uuid.UUID) -> Agent | None:
    return await session.get(Agent, agent_id)


async def agent_has_active_deployment(session: AsyncSession, agent_id: uuid.UUID) -> bool:
    result = await session.scalar(
        select(Deployment.id)
        .where(Deployment.agent_id == agent_id, Deployment.status == "active")
        .limit(1)
    )
    return result is not None


async def delete_agent(session: AsyncSession, agent_id: uuid.UUID) -> None:
    # Remove child rows first, then the agent. Bulk deletes bypass the ORM
    # relationship cascade (which would emit an async lazy-load during flush) and
    # match the FK ondelete=CASCADE already declared on every child table. Bundle
    # objects in RustFS are intentionally left in place (out of scope).
    await session.execute(delete(AgentChannel).where(AgentChannel.agent_id == agent_id))
    await session.execute(delete(Deployment).where(Deployment.agent_id == agent_id))
    await session.execute(delete(AgentVersion).where(AgentVersion.agent_id == agent_id))
    await session.execute(delete(Agent).where(Agent.id == agent_id))
    await session.commit()


async def update_agent_binding(
    session: AsyncSession, agent: Agent, channel: ChannelBindingWrite
) -> Agent:
    """Move the agent's single binding to a new kind/address (ADR-0096, #1459).

    Mutated IN PLACE rather than replaced: an agent holds exactly one binding
    (`agent_channels_agent_id_key`), and assigning a fresh row would make the
    insert of the replacement race the delete of the original inside one flush,
    tripping that constraint on a move that is perfectly legal.

    That in-place mutation is exactly why `generation` exists (ADR-0096 D5): the
    row id is a stable identity, so a credential minted against this binding
    before the move stays pointed at the row afterwards and would follow it to
    its NEW owner. The generation is what makes the rebind observable to that
    credential. It is bumped UNCONDITIONALLY on any binding write, including one
    whose values are identical -- an operator re-asserting a binding is the "I
    think something is wrong with this route" gesture that should invalidate
    outstanding credentials, and guarding the bump on a value change would leave
    that case silently valid.
    """

    agent.channel.kind = channel.kind
    agent.channel.address = channel.address
    # The reply route moves WITH the binding (ADR-0096 phase 2): a PATCH that
    # re-points the pair and leaves the old endpoint/adapter behind would send
    # the new route's replies to the previous adapter, authenticated as it. This
    # is also the cutover's step 10 -- bind first, PATCH the route in later.
    agent.channel.endpoint = channel.endpoint
    agent.channel.adapter = channel.adapter
    agent.channel.generation += 1
    return await _refresh_with_channel(session, agent)


async def update_agent_model(session: AsyncSession, agent: Agent, model: str | None) -> Agent:
    agent.model = model
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_thinking(
    session: AsyncSession, agent: Agent, thinking: str | None
) -> Agent:
    agent.thinking = thinking
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_approval_tools(
    session: AsyncSession, agent: Agent, tools: list[str]
) -> Agent:
    """Set the agent's permission gates (#245). An empty list clears them
    (stored as NULL, the no-gates posture)."""

    agent.approval_required_tools = tools or None
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_approval_routes(
    session: AsyncSession, agent: Agent, routes: dict[str, Any]
) -> Agent:
    """Set the agent's approval route bindings (#247). An empty dict clears
    them (stored as NULL: unbound routes fall back to the requesting channel)."""

    agent.approval_routes = routes or None
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_budget(
    session: AsyncSession,
    agent: Agent,
    max_usd_per_day: float | None,
    max_output_tokens_per_run: int | None,
) -> Agent:
    agent.max_usd_per_day = max_usd_per_day
    agent.max_output_tokens_per_run = max_output_tokens_per_run
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_behavior_packs(
    session: AsyncSession, agent: Agent, behavior_packs: dict[str, Any] | None
) -> Agent:
    agent.behavior_packs = behavior_packs
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_secrets(
    session: AsyncSession, agent: Agent, secrets: dict[str, str] | None
) -> Agent:
    """Set the per-agent connector secrets (#429). An empty dict clears them."""
    agent.secrets = secrets
    await session.commit()
    await session.refresh(agent)
    return agent


async def get_agent_by_repo(session: AsyncSession, repo_full_name: str) -> Agent | None:
    agent: Agent | None = await session.scalar(
        select(Agent).where(Agent.repo_full_name == repo_full_name)
    )
    return agent


async def update_agent_repo(session: AsyncSession, agent: Agent, repo_full_name: str) -> Agent:
    agent.repo_full_name = repo_full_name
    await session.commit()
    await session.refresh(agent)
    return agent


async def get_agents_by_repo(session: AsyncSession, repo_full_name: str) -> list[Agent]:
    """Every agent built from this repository (ADR-0091).

    One repository legitimately binds several agents -- a dev bot and a prod
    bot are the same bundle on two channels. Ordered by name so a caller that
    must pick one without a target (a bundle predating ``deploy.yaml``) picks
    the same one every time rather than whatever the planner returned.
    """

    result = await session.scalars(
        select(Agent).where(Agent.repo_full_name == repo_full_name).order_by(Agent.name)
    )
    return list(result)


async def get_agent_by_name(session: AsyncSession, name: str) -> Agent | None:
    agent: Agent | None = await session.scalar(select(Agent).where(Agent.name == name))
    return agent


async def create_version_row(
    session: AsyncSession,
    agent_id: uuid.UUID,
    version_label: str,
    created_by: str,
    commit_sha: str | None = None,
    bundle_ref: str | None = None,
) -> AgentVersion:
    version = AgentVersion(
        agent_id=agent_id,
        version_label=version_label,
        created_by=created_by,
        commit_sha=commit_sha,
        bundle_ref=bundle_ref,
    )
    session.add(version)
    await session.commit()
    await session.refresh(version)
    return version


async def create_version(
    session: AsyncSession, agent_id: uuid.UUID, data: VersionCreate
) -> AgentVersion:
    return await create_version_row(
        session,
        agent_id,
        version_label=data.version_label,
        created_by=data.created_by,
        bundle_ref=data.bundle_ref,
    )


async def get_version_by_commit(
    session: AsyncSession, agent_id: uuid.UUID, commit_sha: str
) -> AgentVersion | None:
    version: AgentVersion | None = await session.scalar(
        select(AgentVersion).where(
            AgentVersion.agent_id == agent_id,
            AgentVersion.commit_sha == commit_sha,
        )
    )
    return version


async def list_versions(session: AsyncSession, agent_id: uuid.UUID) -> list[AgentVersion]:
    result = await session.scalars(
        select(AgentVersion)
        .where(AgentVersion.agent_id == agent_id)
        .order_by(AgentVersion.created_at)
    )
    return list(result)


async def create_deployment_row(
    session: AsyncSession,
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    environment: Environment,
    commit_sha: str | None = None,
    status: str = "active",
) -> Deployment:
    deployment = Deployment(
        agent_id=agent_id,
        version_id=version_id,
        environment=environment,
        commit_sha=commit_sha,
        status=status,
    )
    session.add(deployment)
    await session.commit()
    await session.refresh(deployment)
    return deployment


async def create_deployment(session: AsyncSession, data: DeploymentCreate) -> Deployment:
    return await create_deployment_row(
        session,
        agent_id=data.agent_id,
        version_id=data.version_id,
        environment=data.environment,
        status=data.status,
    )


async def get_active_deployment(
    session: AsyncSession, agent_id: uuid.UUID, environment: Environment
) -> Deployment | None:
    """The agent's current active deployment in an environment (most recent).

    Git-flow appends a new active Deployment row per push without superseding
    older ones, so "current" is the latest active row for the environment.
    """

    result: Deployment | None = await session.scalar(
        select(Deployment)
        .where(
            Deployment.agent_id == agent_id,
            Deployment.environment == environment,
            Deployment.status == "active",
        )
        .order_by(Deployment.deployed_at.desc())
        .limit(1)
    )
    return result


async def list_deployments(
    session: AsyncSession, agent_id: uuid.UUID | None = None
) -> list[Deployment]:
    stmt = select(Deployment).order_by(Deployment.deployed_at)
    if agent_id is not None:
        stmt = stmt.where(Deployment.agent_id == agent_id)
    result = await session.scalars(stmt)
    return list(result)


async def get_deployment(session: AsyncSession, deployment_id: uuid.UUID) -> Deployment | None:
    return await session.get(Deployment, deployment_id)


# -- approvals (#244, ADR-0010) -------------------------------------------------


async def create_approval(session: AsyncSession, data: "ApprovalRequest") -> Approval:
    """Insert a pending approval. Raises IntegrityError on a dedupe_key replay;
    the router maps that to the existing record (idempotent creation)."""

    expires_at = None
    if data.expires_in_seconds is not None:
        # Naive UTC, matching the DateTime columns (server_default func.now()
        # stores naive timestamps in the session timezone, UTC in this stack).
        expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(
            seconds=data.expires_in_seconds
        )
    approval = Approval(
        agent_id=data.agent_id,
        conversation_id=data.conversation_id,
        author=data.author,
        summary=data.summary,
        # The durable twin of the turn's routing pair and egress selector
        # (ADR-0096 phase 2). Persisted from the request, never re-derived from
        # `agent_channels`: an operator may re-bind the address between
        # suspension and resume, and these are facts about the original turn.
        reply_kind=data.reply_kind,
        reply_channel=data.reply_channel,
        reply_placeholder=data.reply_placeholder,
        reply_endpoint=data.reply_endpoint,
        reply_adapter=data.reply_adapter,
        dedupe_key=data.dedupe_key,
        route=data.route,
        card_channel=data.card_channel,
        gate_kind=data.gate_kind,
        granted_tool=data.granted_tool,
        expires_at=expires_at,
    )
    session.add(approval)
    await session.commit()
    await session.refresh(approval)
    return approval


async def get_approval(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    return await session.get(Approval, approval_id)


async def get_approval_route_binding(session: AsyncSession, approval: Approval) -> Any:
    """The route binding governing ``approval``, read fresh at resolve time
    (#420), or None when there is none to read.

    Read fresh rather than snapshotted at creation: approvals pend for hours to
    days, and evaluating against current policy means removing someone from the
    approver group revokes them immediately instead of leaving them able to
    resolve yesterday's stale request.

    None covers every legitimate miss -- a generic approval with no agent
    (``agent_id`` is nullable by design), an approval with no route, an agent
    with no bindings, a route the map does not bind -- and each of them means
    "no approvers declared", which is channel membership by design (AC4), not a
    failure.

    A present-but-non-dict value is NOT one of those misses, so it is returned
    raw (the JSONB value can be anything) rather than coerced to None: the
    selector fails a malformed binding closed, the same as a malformed
    ``approvers`` block, instead of widening it to card-channel membership.
    """

    if approval.agent_id is None or not approval.route:
        return None
    agent = await get_agent(session, approval.agent_id)
    if agent is None or not isinstance(agent.approval_routes, dict):
        return None
    return agent.approval_routes.get(approval.route)


async def get_approval_by_dedupe_key(session: AsyncSession, dedupe_key: str) -> Approval | None:
    result: Approval | None = await session.scalar(
        select(Approval).where(Approval.dedupe_key == dedupe_key)
    )
    return result


# -- delegation (ADR-0115 PROTOTYPE, see docs/demo/ADR-0115-PROTOTYPE-NOTES.md) --


async def create_delegation_call(
    session: AsyncSession,
    *,
    caller: Agent,
    target: Agent,
    data: DelegateCallIn,
) -> DelegationCall:
    """Snapshot the caller's reply route and insert a pending call.

    ``caller.channel`` fields are copied verbatim, the same reconstruction
    ``hooks._mint_turn`` performs for every hook-originated turn -- the
    durable-twin-of-ReplyHandle pattern ``Approval`` already uses.
    """

    call = DelegationCall(
        caller_agent_id=caller.id,
        caller_conversation_id=data.caller_conversation_id,
        caller_reply_kind=caller.channel.kind,
        caller_reply_channel=caller.channel.address,
        caller_reply_endpoint=caller.channel.endpoint,
        caller_reply_adapter=caller.channel.adapter,
        target_agent_id=target.id,
        request_text=data.message,
    )
    session.add(call)
    await session.commit()
    await session.refresh(call)
    return call


async def get_delegation_call(session: AsyncSession, call_id: uuid.UUID) -> DelegationCall | None:
    return await session.get(DelegationCall, call_id)


async def list_delegation_calls_for_agent(
    session: AsyncSession, agent_id: uuid.UUID
) -> list[DelegationCall]:
    """Every call agent_id was either the caller or the target of, newest
    first. Demo/ops convenience -- not part of the ADR's design."""

    result = await session.scalars(
        select(DelegationCall)
        .where(
            (DelegationCall.caller_agent_id == agent_id)
            | (DelegationCall.target_agent_id == agent_id)
        )
        .order_by(DelegationCall.created_at.desc())
    )
    return list(result)


async def update_delegation_call_text(
    session: AsyncSession, call: DelegationCall, result_text: str
) -> DelegationCall:
    call.result_text = result_text
    await session.commit()
    await session.refresh(call)
    return call


async def resolve_delegation_call(
    session: AsyncSession, call: DelegationCall, *, status: str
) -> DelegationCall:
    """Mark a call resolved. Does not touch ``result_text``: any answer text was
    already buffered by ``update_delegation_call_text`` -- the terminal outcome
    event carries none of its own."""

    call.status = status
    call.resolved_at = datetime.now(UTC).replace(tzinfo=None)
    await session.commit()
    await session.refresh(call)
    return call


async def get_delegate_grant(
    session: AsyncSession, *, caller_agent_id: uuid.UUID, target_agent_id: uuid.UUID
) -> DelegateGrant | None:
    grant: DelegateGrant | None = await session.scalar(
        select(DelegateGrant).where(
            DelegateGrant.caller_agent_id == caller_agent_id,
            DelegateGrant.target_agent_id == target_agent_id,
        )
    )
    return grant


async def upsert_delegate_grant(
    session: AsyncSession, *, caller: Agent, target: Agent, armed: bool
) -> DelegateGrant:
    grant = await get_delegate_grant(
        session, caller_agent_id=caller.id, target_agent_id=target.id
    )
    if grant is None:
        grant = DelegateGrant(caller_agent_id=caller.id, target_agent_id=target.id, armed=armed)
        session.add(grant)
    else:
        grant.armed = armed
    await session.commit()
    await session.refresh(grant)
    return grant


async def list_approvals(
    session: AsyncSession,
    *,
    status: str | None = None,
    agent_id: uuid.UUID | None = None,
    conversation_id: str | None = None,
    limit: int = 50,
) -> list[Approval]:
    stmt = select(Approval).order_by(Approval.created_at.desc()).limit(limit)
    if status is not None:
        stmt = stmt.where(Approval.status == status)
    if agent_id is not None:
        stmt = stmt.where(Approval.agent_id == agent_id)
    if conversation_id is not None:
        stmt = stmt.where(Approval.conversation_id == conversation_id)
    result = await session.scalars(stmt)
    return list(result)


async def claim_approval_resolution(
    session: AsyncSession,
    approval_id: uuid.UUID,
    *,
    decision: str,
    resolved_by: str,
    note: str | None,
) -> Approval | None:
    """The resolve-once compare-and-set: exactly one resolver wins.

    A conditional UPDATE guarded on ``status = 'pending'`` claims the record;
    concurrent attempts see zero rows updated and get None back (the router
    tells them who won). This is the claim-race primitive of ADR-0010.
    """

    result = await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.status == ApprovalStatus.pending)
        .values(
            status=decision,
            resolved_by=resolved_by,
            resolution_note=note,
            resolved_at=func.now(),
        )
        .returning(Approval.id)
    )
    claimed = result.scalar_one_or_none()
    await session.commit()
    if claimed is None:
        return None
    approval = await session.get(Approval, approval_id)
    if approval is not None:
        await session.refresh(approval)
    return approval


async def list_expired_pending_approvals(
    session: AsyncSession, *, now: datetime, limit: int = 100
) -> list[Approval]:
    """The pending approvals whose SLA has lapsed (#412), oldest-lapse-first.

    ``now`` is naive UTC, matching the DateTime columns and the router's
    ``_expired`` comparison. Ordering by ``expires_at`` drains the oldest
    lapses first, so a backlog larger than ``limit`` clears across successive
    sweep passes rather than starving the earliest-expired records. Records
    with a NULL ``expires_at`` (no SLA) are never selected.
    """

    result = await session.scalars(
        select(Approval)
        .where(
            Approval.status == ApprovalStatus.pending,
            Approval.expires_at.is_not(None),
            Approval.expires_at <= now,
        )
        .order_by(Approval.expires_at)
        .limit(limit)
    )
    return list(result)


async def expire_approval(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    """Flip a pending approval past its SLA to expired (same CAS guard, so an
    in-flight resolution that already won is never overwritten)."""

    result = await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.status == ApprovalStatus.pending)
        .values(status=ApprovalStatus.expired, resolved_at=func.now())
        .returning(Approval.id)
    )
    claimed = result.scalar_one_or_none()
    await session.commit()
    if claimed is None:
        return None
    return await session.get(Approval, approval_id)


async def mark_approval_resumed(session: AsyncSession, approval_id: uuid.UUID) -> None:
    """Record that the resume turn made it onto the stream (#411).

    Conditional UPDATE guarded on ``resumed_at IS NULL``, so a second call (a
    reconciler racing the inline path, another replica) matches zero rows and is
    a no-op. Mirrors the conditional-UPDATE style of ``claim_approval_resolution``.
    """

    await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.resumed_at.is_(None))
        .values(resumed_at=func.now())
    )
    await session.commit()


async def reopen_dead_lettered_resume(
    session: AsyncSession, approval_id: uuid.UUID, *, dead_lettered_after: datetime
) -> bool:
    """Re-open an approval whose DELIVERED resume turn was dead-lettered (#532).

    A resume turn that reached the runs stream (so ``resumed_at`` was marked)
    can still die at the worker's delivery cap (#505) and be moved to the
    graveyard, acked off, and never woken -- a row the NULL-gated finder cannot
    re-select. Clearing ``resumed_at`` puts it back on the reconciler's owed-wake
    work-list so the standard reconcile pass re-enqueues it. Conditional UPDATE
    mirroring ``mark_approval_resumed``; returns whether a row was re-opened.

    The ``resumed_at < dead_lettered_after`` guard is LOAD-BEARING for
    idempotency: it fires only when the CURRENTLY-marked wake predates THIS
    dead-letter, so a graveyard row that persists across passes (the stream is
    only approximately trimmed) cannot repeatedly re-open a row that has since
    been re-enqueued -- its new ``resumed_at`` is newer than the row's
    dead-letter time. A genuinely new dead-letter carries a newer time and
    re-triggers. A row already re-opened (``resumed_at`` NULL) matches zero rows,
    so the standard NULL-gated reconciler owns it, never this path.

    The comparison is a CROSS-NODE clock comparison: ``resumed_at`` is stamped
    by Postgres (``func.now()`` on the inline mark path) or the API pod clock
    (``datetime.now(UTC)`` on the reconcile re-enqueue path), while
    ``dead_lettered_after`` is the worker pod's clock (``dl_dead_lettered_at``).
    It is safe because the gap between marking a wake and exhausting the
    delivery cap is minutes, dwarfing realistic NTP skew.
    """

    result = await session.execute(
        update(Approval)
        .where(
            Approval.id == approval_id,
            Approval.status.in_(_RESUMABLE_STATUSES),
            Approval.resumed_at.is_not(None),
            Approval.resumed_at < dead_lettered_after,
        )
        .values(resumed_at=None)
        .returning(Approval.id)
    )
    reopened = result.scalar_one_or_none() is not None
    await session.commit()
    return reopened


# The statuses an owed-wake row can carry: a terminal outcome that must still
# reach its suspended session. ``expired`` belongs here since #412 gave both
# expiry paths (the sweeper and the resolve-path expiry branch) a resume turn of
# their own, so an expired record owes a wake exactly as a decided one does
# (#418). Only ``pending`` is excluded: it has neither been decided nor lapsed,
# so nothing is owed yet. Shared by the reconciler's candidate finder and its
# per-row claim so the two never desync.
_RESUMABLE_STATUSES = (
    ApprovalStatus.approved,
    ApprovalStatus.rejected,
    ApprovalStatus.expired,
)


async def claim_resume_row(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    """Atomically claim one owed-wake row for this reconcile pass (#411).

    ``SELECT ... FOR UPDATE SKIP LOCKED`` locks the row for the caller's
    transaction, or returns None if another replica already holds it OR it is
    already resumed (or no longer resolved). This is the per-row claim that keeps
    two API replicas' overlapping reconcile passes from both enqueuing the same
    resume turn -- the worker's done-marker is written only post-terminal, so it
    cannot dedupe a concurrent re-run; the row claim must. The caller owns the
    transaction (this does NOT commit); marking ``resumed_at`` on the returned
    ORM object and committing releases the lock.
    """

    approval: Approval | None = await session.scalar(
        select(Approval)
        .where(
            Approval.id == approval_id,
            Approval.resumed_at.is_(None),
            Approval.status.in_(_RESUMABLE_STATUSES),
        )
        .with_for_update(skip_locked=True)
    )
    return approval


async def list_resolved_unresumed(
    session: AsyncSession, *, resolved_before: datetime, limit: int
) -> list[uuid.UUID]:
    """The reconciler's work-list: ids of settled approvals whose wake is owed.

    A row in any ``_RESUMABLE_STATUSES`` with ``resolved_at`` set and
    ``resumed_at`` NULL is an owed wake: every path that settles a record (the
    resolve endpoint, the expiry sweeper, and the resolve-path expiry branch)
    enqueues a resume turn and marks ``resumed_at`` only once that enqueue
    succeeded, so NULL means the wake never reached the stream. That now includes
    ``expired`` records (#418), whose expiry wake was previously unrecoverable
    because a flipped record is no longer ``pending`` and so is never re-selected
    by ``list_expired_pending_approvals``. ``resolved_before`` is naive UTC,
    matching the DateTime columns.

    Returns ids only (the unlocked candidate finder): each id is then claimed
    atomically by ``claim_resume_row`` in its own short transaction, which
    re-reads the row under lock, so the reconciler never holds a row lock across
    the Valkey enqueue of the batch and never needs the full row here.
    """

    result = await session.scalars(
        select(Approval.id)
        .where(
            Approval.status.in_(_RESUMABLE_STATUSES),
            Approval.resolved_at.is_not(None),
            Approval.resumed_at.is_(None),
            Approval.resolved_at <= resolved_before,
        )
        .order_by(Approval.resolved_at)
        .limit(limit)
    )
    return list(result)


async def append_approval_audit(
    session: AsyncSession,
    *,
    approval_id: uuid.UUID,
    action: str,
    actor: str,
    actor_channel: str | None,
    decision: str,
    authorizer: str,
    authorized: bool,
    reason: str | None,
    evidence: dict[str, Any] | None = None,
) -> ApprovalAuditEntry:
    """Append one audit row (#247). Append-only by design; never updated.

    ``evidence`` (#420) is the membership snapshot the authorizer decided on;
    None for writers that made no membership decision.
    """

    entry = ApprovalAuditEntry(
        approval_id=approval_id,
        action=action,
        actor=actor,
        actor_channel=actor_channel,
        decision=decision,
        authorizer=authorizer,
        authorized=authorized,
        reason=reason,
        evidence=evidence,
    )
    session.add(entry)
    await session.commit()
    await session.refresh(entry)
    return entry


async def list_approval_audit(
    session: AsyncSession, approval_id: uuid.UUID
) -> list[ApprovalAuditEntry]:
    result = await session.scalars(
        select(ApprovalAuditEntry)
        .where(ApprovalAuditEntry.approval_id == approval_id)
        .order_by(ApprovalAuditEntry.created_at)
    )
    return list(result)


# --- console sessions (ADR-0083, #1044) -------------------------------------
#
# The credential never enters the database: every read and write below goes
# through `hash_console_credential`, so a dump of `console_sessions` is useless to
# an attacker. Callers hold the plaintext only long enough to hand it to the
# operator (the code) or the browser (the token).

#: How long a minted login code stays redeemable. Short by design: it exists only
#: to be copied from a terminal into a browser once.
LOGIN_CODE_TTL = timedelta(minutes=10)

#: How long an established console session stays valid before a fresh login.
CONSOLE_SESSION_TTL = timedelta(hours=12)


def hash_console_credential(value: str) -> str:
    """The stored form of a login code or session token.

    SHA-256 rather than a password hash on purpose: these are high-entropy
    machine-generated values, not user-chosen secrets, so there is nothing to slow
    down a guessing attack against -- and a lookup happens on every session-authed
    request, where a deliberately slow KDF would be a denial-of-service surface.

    Args:
        value: The plaintext code or token.

    Returns:
        Lowercase hex SHA-256 of ``value``.
    """
    return hashlib.sha256(value.encode()).hexdigest()


def new_login_code() -> str:
    """A single-use login code an operator can copy out of a terminal."""
    return secrets.token_urlsafe(12)


def new_session_token() -> str:
    """A console session token. Longer than the code: it is never typed."""
    return secrets.token_urlsafe(32)


async def create_console_login_code(
    session: AsyncSession, *, now: datetime | None = None
) -> tuple[str, ConsoleSession]:
    """Mint a login code and its pending session row.

    Args:
        session: The database session.
        now: Injectable clock, so expiry is testable without sleeping.

    Returns:
        ``(plaintext code, row)``. The plaintext is returned ONCE and never
        stored; only its hash is persisted.
    """
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    code = new_login_code()
    row = ConsoleSession(
        login_code_hash=hash_console_credential(code),
        login_code_expires_at=moment + LOGIN_CODE_TTL,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return code, row


async def exchange_console_login_code(
    session: AsyncSession, code: str, *, now: datetime | None = None
) -> tuple[str, ConsoleSession] | None:
    """Consume a login code and mint the session token it establishes.

    Single-use and expiry are enforced HERE rather than by the caller, so no
    endpoint can accidentally skip either. A code that is unknown, already
    consumed, expired, or whose row was revoked yields ``None`` -- one
    indistinguishable failure, so a caller cannot probe which codes exist.

    Args:
        session: The database session.
        code: The plaintext login code presented by the browser.
        now: Injectable clock.

    Returns:
        ``(plaintext session token, row)`` on success, else ``None``.
    """
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    result = await session.execute(
        select(ConsoleSession).where(
            ConsoleSession.login_code_hash == hash_console_credential(code)
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None
    if row.consumed_at is not None or row.revoked_at is not None:
        return None
    if row.login_code_expires_at <= moment:
        return None

    token = new_session_token()
    row.session_token_hash = hash_console_credential(token)
    row.session_expires_at = moment + CONSOLE_SESSION_TTL
    row.consumed_at = moment
    await session.commit()
    await session.refresh(row)
    return token, row


async def live_console_session(
    session: AsyncSession, token: str, *, now: datetime | None = None
) -> ConsoleSession | None:
    """The session a token authenticates, or ``None`` if it does not authenticate one.

    "Live" means exchanged, unrevoked and unexpired. Slice 2 (#1045) is what calls
    this from `require_api_key`; it lands here with the store so the store's own
    tests can prove revocation and expiry are expressed, per #1044's acceptance.

    Args:
        session: The database session.
        token: The plaintext session token from the cookie.
        now: Injectable clock.

    Returns:
        The live row, else ``None`` -- again one indistinguishable failure.
    """
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    result = await session.execute(
        select(ConsoleSession).where(
            ConsoleSession.session_token_hash == hash_console_credential(token)
        )
    )
    row = result.scalar_one_or_none()
    if row is None or row.revoked_at is not None:
        return None
    if row.session_expires_at is None or row.session_expires_at <= moment:
        return None
    return row


async def revoke_console_session(
    session: AsyncSession, row: ConsoleSession, *, now: datetime | None = None
) -> ConsoleSession:
    """Revoke a session by stamping ``revoked_at``.

    A column write, which is the whole point of a stored session: the operator can
    kill one without rotating the platform key and restarting the API.
    """
    row.revoked_at = now or datetime.now(UTC).replace(tzinfo=None)
    await session.commit()
    await session.refresh(row)
    return row

