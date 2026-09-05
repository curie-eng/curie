"""Database access helpers for agents, versions, deployments, and approvals."""

import hashlib
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from channel_protocol import scoped_conversation_id
from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .config import get_settings
from .models import (
    ActionAuditEntry,
    ActionStatus,
    Agent,
    AgentAction,
    AgentChannel,
    AgentVersion,
    Approval,
    ApprovalAuditEntry,
    ApprovalStatus,
    ConsoleSession,
    CredentialRedemptionAuditEntry,
    Deployment,
    Environment,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadWorkspace,
)
from .publication_authority import VerifiedPublicationIdentity
from .schemas import (
    ActionComplete,
    ActionRecord,
    AgentCreate,
    ApprovalRequest,
    ChannelBindingPatch,
    ChannelBindingWrite,
    DeploymentCreate,
    HookPartitionConfig,
    PublicationCreate,
    PublicationLineageAdvance,
    ReviewRevisionReserve,
    VersionCreate,
)
from .workspace_policy import repository_is_allowed

_WORKSPACE_UNSET = object()


class PublicationReplayConflict(RuntimeError):
    """A publication dedupe key was replayed with different private facts."""


class PublicationLineageConflict(RuntimeError):
    """A publication revision cannot safely mutate its thread lineage."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


async def _adopt_publication_replay(
    session: AsyncSession,
    data: PublicationCreate,
    patch: bytes,
) -> Publication | None:
    """Adopt an exact replay only through its persisted authorization lane."""

    approval = await get_approval_by_dedupe_key(session, data.dedupe_key)
    if approval is None:
        return None
    publication = await get_publication_by_approval(session, approval.id)
    if publication is None:
        raise PublicationReplayConflict(
            "publication dedupe key belongs to a non-publication approval"
        )
    deployment = await get_deployment(session, data.deployment_id)
    if deployment is None:
        raise LookupError("deployment not found")
    review_origin = await session.scalar(
        select(PublicationReviewReservation.origin_key).where(
            PublicationReviewReservation.id == publication.id
        )
    )
    if review_origin != data.review_origin_key:
        raise PublicationReplayConflict("publication replay has a different review origin")
    reply_conversation_id = data.reply_conversation_id or data.conversation_id
    workspace_conversation_id = (
        data.conversation_id
        if data.reply_conversation_id is not None
        else scoped_conversation_id(
            data.reply_kind,
            data.reply_channel,
            data.conversation_id,
        )
    )
    if (
        approval.agent_id != deployment.agent_id
        or approval.conversation_id != reply_conversation_id
        or approval.reply_kind != data.reply_kind
        or approval.reply_channel != data.reply_channel
        or approval.reply_placeholder != data.reply_placeholder
        or approval.reply_endpoint != data.reply_endpoint
        or approval.reply_adapter != data.reply_adapter
        or publication.deployment_id != data.deployment_id
        or publication.repo_full_name.casefold() != data.repo_full_name.casefold()
        or publication.base_sha != data.base_sha
        or publication.patch_bytes != patch
        or publication.changed_paths != data.changed_paths
        or publication.title != (data.title or data.summary)
        or publication.body != (data.body or "Approved platform publication.")
        or publication.reply_kind != data.reply_kind
        or publication.reply_channel != data.reply_channel
        or publication.reply_placeholder != data.reply_placeholder
        or publication.reply_endpoint != data.reply_endpoint
        or publication.reply_adapter != data.reply_adapter
        or publication.lineage is None
        or publication.lineage.agent_id != deployment.agent_id
        or publication.lineage.conversation_id != workspace_conversation_id
        or publication.lineage.repo_full_name.casefold() != publication.repo_full_name.casefold()
    ):
        raise PublicationReplayConflict(
            "publication dedupe key was replayed with different snapshot facts"
        )
    if publication.workspace_conversation_id is None:
        # 0041 canonicalizes every genuinely legacy row. A NULL observed after
        # that migration is corruption or an artificial downgrade lane, never
        # authority to fall back to an adapter-native reply id.
        raise PublicationReplayConflict("publication replay has no canonical workspace identity")
    authorized_conversation_id = publication.workspace_conversation_id
    await _require_current_publication_workspace(
        session,
        data,
        conversation_id=authorized_conversation_id,
        deployment=deployment,
    )
    return publication


async def _require_current_publication_workspace(
    session: AsyncSession,
    data: PublicationCreate,
    *,
    conversation_id: str,
    deployment: Deployment | None = None,
) -> tuple[Deployment, ThreadWorkspace]:
    """Authorize the request against current deployment and thread policy."""

    if deployment is None:
        deployment = await get_deployment(session, data.deployment_id)
    if deployment is None:
        raise LookupError("deployment not found")
    thread_workspace = await get_thread_workspace(
        session,
        agent_id=deployment.agent_id,
        conversation_id=conversation_id,
    )
    if thread_workspace is None:
        raise ValueError("conversation has no selected repository workspace")
    if thread_workspace.repo_full_name.casefold() != data.repo_full_name.casefold():
        raise ValueError("publication repository differs from the thread workspace")
    if not repository_is_allowed(
        thread_workspace.repo_full_name, get_settings().github_repo_allowlist
    ):
        raise ValueError("thread workspace repository is no longer allowed")
    return deployment, thread_workspace


async def get_version(session: AsyncSession, version_id: uuid.UUID) -> AgentVersion | None:
    return await session.get(AgentVersion, version_id)


async def refresh_with_channels(session: AsyncSession, agent: Agent) -> Agent:
    """Commit, then refresh the agent and name the `channels` relationship explicitly.

    The response model reads `agent.channels` after this session is done with,
    and an unloaded relationship RAISES under asyncio rather than lazy-loading,
    so the endpoint 500s where a crud-level test (holding a live session) passes.
    Naming the collection also re-reads it from the database, so a binding
    inserted or deleted around the relationship is reflected rather than served
    from the stale loaded collection (`expire_on_commit=False`).
    """
    await session.commit()
    await session.refresh(agent)
    await session.refresh(agent, ["channels"])
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
        # A create binds exactly ONE channel (ADR-0118 keeps the create
        # singular); the rest arrive through `add_channel_binding`.
        channels=[
            AgentChannel(
                kind=data.channel.kind,
                address=data.channel.address,
                endpoint=data.channel.endpoint,
                adapter=data.channel.adapter,
            )
        ],
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
        hook_partitions=_stored_hook_partitions(data.hook_partitions),
        secrets=data.secrets,
        memory=data.memory,
    )
    session.add(agent)
    return await refresh_with_channels(session, agent)


async def list_agents(session: AsyncSession) -> list[Agent]:
    # `selectinload` explicitly, even though the relationship is already
    # lazy="selectin": the list path is the one where the per-row alternative is
    # correct AND unboundedly slow, so a hundred-agent install would pay a
    # hundred round trips to render one page.
    result = await session.scalars(
        select(Agent).options(selectinload(Agent.channels)).order_by(Agent.created_at)
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


async def lock_agent_bindings(session: AsyncSession, agent_id: uuid.UUID) -> list[AgentChannel]:
    """`SELECT ... FOR UPDATE` the agent's WHOLE binding set, ordered as it reads.

    Every mutating binding handler opens with this, and then picks its target
    out of the returned list rather than issuing a second, unlocked query --
    which is what makes the lock load-bearing instead of decorative.

    Without it the last-binding guard is unsound: an agent with two bindings and
    two concurrent DELETEs of DIFFERENT pairs has both requests read count=2,
    both pass the guard, and the agent lands at ZERO bindings -- deployed,
    healthy-looking, answering nothing (#38). Under the lock the second delete
    re-reads count=1 and conflicts. The lock also serializes `generation += 1`
    into an increment instead of a lost update.

    `populate_existing` is load-bearing: the handler has already loaded the
    agent (for its 404), so its bindings are in the session's identity map, and
    a plain locking SELECT would hand those STALE objects back -- the row would
    be locked while the generation the caller compares against came from before
    the winner's commit.

    Known and accepted conservatism: `FOR UPDATE` locks rows that exist; it does
    not block a concurrent INSERT. A DELETE racing an ADD may therefore 409 as
    "last binding" even though a second binding commits moments later. That
    direction is safe (a retry succeeds) and is cheaper than the predicate lock
    that would close it -- it is accepted, not overlooked.
    """

    result = await session.scalars(
        select(AgentChannel)
        .where(AgentChannel.agent_id == agent_id)
        .order_by(AgentChannel.kind, AgentChannel.address)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(result)


async def agent_id_for_pair(session: AsyncSession, kind: str, address: str) -> uuid.UUID | None:
    """Which agent holds this `(kind, address)` pair, if any.

    Named rather than inlined at its one call site: it answers the question a
    binding write's 409 has to answer accurately -- is the duplicate THIS
    agent's or another's -- and an inline `select` there reads as an incidental
    query the next reader deletes.
    """

    owner: uuid.UUID | None = await session.scalar(
        select(AgentChannel.agent_id).where(
            AgentChannel.kind == kind, AgentChannel.address == address
        )
    )
    return owner


async def update_channel_binding(
    session: AsyncSession, binding: AgentChannel, channel: ChannelBindingPatch
) -> AgentChannel:
    """Move ONE binding row to a new kind/address (ADR-0096, #1459; ADR-0118).

    Mutated IN PLACE rather than replaced: assigning a fresh row would make the
    insert of the replacement race the delete of the original inside one flush,
    tripping `agent_channels_kind_address_key` on a move that is perfectly
    legal.

    That in-place mutation is exactly why `generation` exists (ADR-0096 D5): the
    row id is a stable identity, so a credential minted against this binding
    before the move stays pointed at the row afterwards and would follow it to
    its NEW owner. The generation is what makes the rebind observable to that
    credential. It is bumped UNCONDITIONALLY on any binding write, including one
    whose values are identical -- an operator re-asserting a binding is the "I
    think something is wrong with this route" gesture that should invalidate
    outstanding credentials, and guarding the bump on a value change would leave
    that case silently valid.

    FLUSHES rather than commits, so the caller can run it inside a SAVEPOINT:
    the unique violation this raises has to be recoverable without discarding
    the outer transaction's `FOR UPDATE` locks.
    """

    binding.kind = channel.kind
    binding.address = channel.address
    # The reply route moves WITH the pair (ADR-0096 phase 2): a move that
    # re-points the pair and leaves the old endpoint/adapter behind would send
    # the new route's replies to the previous adapter, authenticated as it. This
    # is also the cutover's step 10 -- bind first, move the route in later.
    if "endpoint" in channel.model_fields_set:
        binding.endpoint = channel.endpoint
        binding.adapter = channel.adapter
    binding.generation += 1
    await session.flush()
    return binding


async def add_channel_binding(
    session: AsyncSession, agent_id: uuid.UUID, channel: ChannelBindingWrite
) -> AgentChannel:
    """Append a binding to an agent (ADR-0118). Appends; never moves.

    A new row, so its `generation` starts at 0 and no credential can exist for
    it yet. Flushes for the same savepoint reason as `update_channel_binding`.
    """

    binding = AgentChannel(
        agent_id=agent_id,
        kind=channel.kind,
        address=channel.address,
        endpoint=channel.endpoint,
        adapter=channel.adapter,
    )
    session.add(binding)
    await session.flush()
    return binding


async def delete_channel_binding(session: AsyncSession, binding: AgentChannel) -> None:
    """Remove one binding row, after the caller proved under the lock that it is
    not the agent's last one.

    Deleting the row invalidates its outstanding channel tokens by construction:
    a `chn` claim names `channel_id`, and the id no longer resolves. The
    siblings' tokens are untouched, because the counters and ids are per-row.
    """

    await session.delete(binding)
    await session.flush()


async def update_agent_model(session: AsyncSession, agent: Agent, model: str | None) -> Agent:
    agent.model = model
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_thinking(session: AsyncSession, agent: Agent, thinking: str | None) -> Agent:
    agent.thinking = thinking
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_memory(session: AsyncSession, agent: Agent, memory: bool) -> Agent:
    """Set whether this agent's bindings share one workflow-state namespace
    (#1525 follow-up). Flipping it changes nothing already stored -- a row
    written under one scope is simply not the row a later request under the
    other scope reads; it is a routing decision for FUTURE state calls, not a
    migration of past ones."""

    agent.memory = memory
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
    them (stored as NULL: unbound routes escalate rather than inventing a
    resolution surface)."""

    agent.approval_routes = routes or None
    await session.commit()
    await session.refresh(agent)
    return agent


def _stored_hook_partitions(
    partitions: dict[str, HookPartitionConfig] | None,
) -> dict[str, Any] | None:
    """The column value for a hook-partition map (ADR-0134).

    One definition for both write paths, because create and PATCH must not
    disagree about what "no configuration" looks like in the column: an empty
    map is stored as NULL, the same "every hook returns to one thread per hook"
    posture as an omitted map, which is what an operator turning the feature
    off is asking for.
    """

    if not partitions:
        return None
    return {name: c.model_dump() for name, c in partitions.items()}


async def update_agent_hook_partitions(
    session: AsyncSession, agent: Agent, partitions: dict[str, HookPartitionConfig]
) -> Agent:
    """Set which of the agent's hooks fan out (ADR-0134). An empty dict clears
    them (stored as NULL)."""

    agent.hook_partitions = _stored_hook_partitions(partitions)
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
        commit_sha=data.commit_sha,
        bundle_ref=data.bundle_ref,
    )


async def get_version_by_commit(
    session: AsyncSession, agent_id: uuid.UUID, commit_sha: str, created_by: str
) -> AgentVersion | None:
    version: AgentVersion | None = await session.scalar(
        select(AgentVersion).where(
            AgentVersion.agent_id == agent_id,
            AgentVersion.commit_sha == commit_sha,
            AgentVersion.created_by == created_by,
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
    workspace_enabled: bool | object = _WORKSPACE_UNSET,
) -> Deployment:
    resolved_workspace_enabled: bool
    if workspace_enabled is _WORKSPACE_UNSET:
        current = await get_active_deployment(session, agent_id, environment)
        resolved_workspace_enabled = current.workspace_enabled if current is not None else False
    else:
        assert isinstance(workspace_enabled, bool)
        resolved_workspace_enabled = workspace_enabled
    deployment = Deployment(
        agent_id=agent_id,
        version_id=version_id,
        environment=environment,
        commit_sha=commit_sha,
        workspace_enabled=resolved_workspace_enabled,
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
        commit_sha=data.commit_sha,
        status=data.status,
        workspace_enabled=(
            data.workspace_enabled
            if "workspace_enabled" in data.model_fields_set
            else _WORKSPACE_UNSET
        ),
    )


async def get_thread_workspace(
    session: AsyncSession, *, agent_id: uuid.UUID, conversation_id: str
) -> ThreadWorkspace | None:
    selected: ThreadWorkspace | None = await session.scalar(
        select(ThreadWorkspace).where(
            ThreadWorkspace.agent_id == agent_id,
            ThreadWorkspace.conversation_id == conversation_id,
        )
    )
    return selected


async def select_thread_workspace(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    deployment_id: uuid.UUID,
    conversation_id: str,
    repo_full_name: str,
    selected_by: str,
) -> tuple[ThreadWorkspace, bool]:
    """Insert the first selection or atomically adopt the concurrent winner."""

    candidate_id = uuid.uuid4()
    inserted = await session.scalar(
        insert(ThreadWorkspace)
        .values(
            id=candidate_id,
            agent_id=agent_id,
            selected_by_deployment_id=deployment_id,
            conversation_id=conversation_id,
            repo_full_name=repo_full_name,
            selected_by=selected_by,
        )
        .on_conflict_do_nothing(constraint="thread_workspaces_agent_conversation_key")
        .returning(ThreadWorkspace.id)
    )
    await session.commit()
    selected = await get_thread_workspace(
        session, agent_id=agent_id, conversation_id=conversation_id
    )
    assert selected is not None
    return selected, inserted == candidate_id


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


async def end_deployment(session: AsyncSession, deployment: Deployment) -> None:
    deployment.status = "stopped"
    await session.commit()


# -- approvals (#244, ADR-0010) -------------------------------------------------


async def create_publication(
    session: AsyncSession,
    data: PublicationCreate,
    *,
    patch: bytes,
    traceparent: str | None = None,
) -> tuple[Publication, bool]:
    """Atomically create the durable approval and its private publication.

    ``dedupe_key`` belongs to Approval, so the replay lookup starts there. An
    exact replay adopts both rows; a changed patch or snapshot fact is a hard
    conflict and can never replace bytes that were already approved.
    """

    existing = await _adopt_publication_replay(session, data, patch)
    if existing is not None:
        await session.refresh(existing, ["lineage"])
        return existing, False

    workspace_conversation_id = (
        data.conversation_id
        if data.reply_conversation_id is not None
        else scoped_conversation_id(
            data.reply_kind,
            data.reply_channel,
            data.conversation_id,
        )
    )
    deployment, thread_workspace = await _require_current_publication_workspace(
        session,
        data,
        conversation_id=workspace_conversation_id,
    )

    lineage = await _get_thread_publication_lineage(
        session,
        agent_id=deployment.agent_id,
        conversation_id=workspace_conversation_id,
        repo_full_name=thread_workspace.repo_full_name,
        for_update=True,
    )
    reservation: PublicationReviewReservation | None = None
    if lineage is None:
        if data.review_origin_key is not None:
            raise PublicationLineageConflict(
                "publication.review_ineligible", "review origin has no existing lineage"
            )
        binding = await session.scalar(
            select(AgentChannel)
            .where(
                AgentChannel.agent_id == deployment.agent_id,
                AgentChannel.kind == data.reply_kind,
                AgentChannel.address == data.reply_channel,
            )
            .with_for_update()
        )
        if binding is not None and (
            binding.endpoint != data.reply_endpoint or binding.adapter != data.reply_adapter
        ):
            binding = None
        lineage_id = uuid.uuid4()
        lineage = ThreadPublicationLineage(
            id=lineage_id,
            agent_id=deployment.agent_id,
            deployment_id=deployment.id,
            conversation_id=workspace_conversation_id,
            repo_full_name=thread_workspace.repo_full_name,
            base_sha=data.base_sha,
            branch=f"curie/publication-{lineage_id.hex}",
            status="open",
            version=1,
            latest_revision=1,
            binding_id=binding.id if binding is not None else None,
            binding_generation=binding.generation if binding is not None else None,
            reply_conversation_id=data.reply_conversation_id or data.conversation_id,
        )
        session.add(lineage)
        try:
            await session.flush()
        except IntegrityError as exc:
            # A distinct first request can race this INSERT. Its approval and
            # publication have not been flushed, so the losing transaction can
            # safely roll back without leaving a second human decision.
            await session.rollback()
            existing = await _adopt_publication_replay(session, data, patch)
            if existing is not None:
                await session.refresh(existing, ["lineage"])
                return existing, False
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "another publication revision already owns this thread lineage",
            ) from exc
        revision_number = 1
    else:
        if lineage.status != "open":
            raise PublicationLineageConflict(
                "publication.lineage_terminal",
                "the pull request for this thread is merged or closed; start a new thread",
            )
        pending_outcome = await session.scalar(
            select(Publication.id).where(
                Publication.lineage_id == lineage.id,
                Publication.status.in_(("denied", "expired", "succeeded", "failed")),
                Publication.outcome_history_ready_at.is_(None),
            )
        )
        if pending_outcome is not None:
            raise PublicationLineageConflict(
                "publication.outcome_pending",
                "the previous publication outcome is not yet durable in thread history",
            )
        expected_prior_head = lineage.head_sha or lineage.base_sha
        if data.base_sha != expected_prior_head:
            raise PublicationLineageConflict(
                "publication.lineage_stale",
                "the managed checkout is not at the current pull request head",
            )
        in_flight = await session.scalar(
            select(Publication.id).where(
                Publication.lineage_id == lineage.id,
                Publication.status.in_(("pending", "approved", "launching", "running")),
            )
        )
        if in_flight is not None:
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "another publication revision is still in progress for this thread",
            )
        reservation = await session.scalar(
            select(PublicationReviewReservation)
            .where(
                PublicationReviewReservation.lineage_id == lineage.id,
                PublicationReviewReservation.status == "reserved",
            )
            .with_for_update()
        )
        if reservation is not None:
            if (
                data.review_origin_key != reservation.origin_key
                or reservation.lineage_version != lineage.version
                or reservation.expected_head_sha != expected_prior_head
            ):
                raise PublicationLineageConflict(
                    "publication.revision_conflict",
                    "a different review origin or stale head owns this revision",
                )
            review_binding = await _require_review_binding(session, lineage)
            if (
                data.reply_kind != review_binding.kind
                or data.reply_channel != review_binding.address
                or data.reply_endpoint != review_binding.endpoint
                or data.reply_adapter != review_binding.adapter
                or (data.reply_conversation_id or data.conversation_id)
                != lineage.reply_conversation_id
            ):
                raise PublicationLineageConflict(
                    "publication.review_ineligible",
                    "review publication reply differs from its reserved original binding",
                )
            revision_number = reservation.revision_number
            reservation.status = "consumed"
            reservation.version += 1
        elif data.review_origin_key is not None:
            raise PublicationLineageConflict(
                "publication.review_ineligible", "review origin has no active reservation"
            )
        else:
            revision_number = lineage.latest_revision + 1
        lineage.latest_revision = revision_number
        lineage.updated_at = func.now()

    expected_prior_head = lineage.head_sha or lineage.base_sha
    expires_at = None
    if data.expires_in_seconds is not None:
        expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(
            seconds=data.expires_in_seconds
        )
    approval = Approval(
        agent_id=deployment.agent_id,
        conversation_id=data.reply_conversation_id or data.conversation_id,
        author=data.author,
        summary=data.summary,
        reply_kind=data.reply_kind,
        reply_channel=data.reply_channel,
        reply_placeholder=data.reply_placeholder,
        reply_endpoint=data.reply_endpoint,
        reply_adapter=data.reply_adapter,
        dedupe_key=data.dedupe_key,
        traceparent=traceparent,
        route=None,
        card_channel=data.reply_channel,
        gate_kind="permission",
        granted_tool="mcp__curie__publish_changes",
        purpose="publication",
        expires_at=expires_at,
    )
    session.add(approval)
    publication = Publication(
        id=reservation.id if reservation is not None else uuid.uuid4(),
        approval=approval,
        deployment_id=deployment.id,
        workspace_conversation_id=workspace_conversation_id,
        lineage=lineage,
        revision_number=revision_number,
        expected_prior_head=expected_prior_head,
        repo_full_name=thread_workspace.repo_full_name,
        status="pending",
        version=1,
        base_sha=data.base_sha,
        patch_bytes=patch,
        changed_paths=data.changed_paths,
        title=data.title or data.summary,
        body=data.body or "Approved platform publication.",
        reply_kind=data.reply_kind,
        reply_channel=data.reply_channel,
        reply_placeholder=data.reply_placeholder,
        reply_endpoint=data.reply_endpoint,
        reply_adapter=data.reply_adapter,
    )
    session.add(publication)
    try:
        await session.commit()
    except IntegrityError as exc:
        # The dedupe key and active-revision indexes arbitrate deliveries that
        # raced after the locked lineage read. Adopt only an exact replay.
        await session.rollback()
        existing = await _adopt_publication_replay(session, data, patch)
        if existing is None:
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "another publication revision already owns this thread lineage",
            ) from exc
        await session.refresh(existing, ["lineage"])
        return existing, False
    await session.refresh(publication)
    await session.refresh(publication, ["lineage"])
    return publication, True


async def get_publication(session: AsyncSession, publication_id: uuid.UUID) -> Publication | None:
    publication: Publication | None = await session.scalar(
        select(Publication)
        .options(selectinload(Publication.lineage))
        .where(Publication.id == publication_id)
    )
    return publication


async def get_publication_by_approval(
    session: AsyncSession, approval_id: uuid.UUID
) -> Publication | None:
    publication: Publication | None = await session.scalar(
        select(Publication)
        .options(selectinload(Publication.lineage))
        .where(Publication.approval_id == approval_id)
    )
    return publication


async def list_publications(session: AsyncSession, *, limit: int = 100) -> list[Publication]:
    result = await session.scalars(
        select(Publication)
        .options(selectinload(Publication.lineage))
        .order_by(Publication.created_at.desc())
        .limit(limit)
    )
    return list(result)


async def _get_thread_publication_lineage(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
    repo_full_name: str,
    for_update: bool = False,
) -> ThreadPublicationLineage | None:
    statement = (
        select(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.agent_id == agent_id,
            ThreadPublicationLineage.conversation_id == conversation_id,
            ThreadPublicationLineage.repo_full_name == repo_full_name,
        )
        .order_by(ThreadPublicationLineage.created_at.desc())
        .limit(1)
    )
    if for_update:
        statement = statement.with_for_update()
    lineage: ThreadPublicationLineage | None = await session.scalar(statement)
    return lineage


async def get_thread_publication_lineage(
    session: AsyncSession,
    *,
    deployment_id: uuid.UUID,
    conversation_id: str,
    repo_full_name: str,
) -> ThreadPublicationLineage | None:
    """Read one authorized thread lineage without exposing credentials."""

    deployment = await get_deployment(session, deployment_id)
    if deployment is None:
        raise LookupError("deployment not found")
    selected = await get_thread_workspace(
        session,
        agent_id=deployment.agent_id,
        conversation_id=conversation_id,
    )
    if selected is None:
        raise ValueError("conversation has no selected repository workspace")
    if selected.repo_full_name.casefold() != repo_full_name.casefold():
        raise ValueError("publication repository differs from the thread workspace")
    if not repository_is_allowed(repo_full_name, get_settings().github_repo_allowlist):
        raise ValueError("thread workspace repository is no longer allowed")
    return await _get_thread_publication_lineage(
        session,
        agent_id=deployment.agent_id,
        conversation_id=conversation_id,
        repo_full_name=selected.repo_full_name,
    )


async def publication_lineage_has_pending_revision(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return only whether private revision work still owns this lineage."""

    if lineage.status != "open":
        return False
    pending_id = await session.scalar(
        select(Publication.id)
        .where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("pending", "approved", "launching", "running")),
        )
        .limit(1)
    )
    if pending_id is not None:
        return True
    return (
        await session.scalar(
            select(PublicationReviewReservation.id)
            .where(
                PublicationReviewReservation.lineage_id == lineage.id,
                PublicationReviewReservation.status == "reserved",
            )
            .limit(1)
        )
        is not None
    )


async def publication_lineage_has_pending_outcome(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return whether a terminal result still owes durable thread history."""

    pending_id = await session.scalar(
        select(Publication.id)
        .where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("denied", "expired", "succeeded", "failed")),
            Publication.outcome_history_ready_at.is_(None),
        )
        .limit(1)
    )
    return pending_id is not None


async def publication_lineage_visible_outcome_revision(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> int:
    """Return the newest revision already present in durable thread history."""

    revision = await session.scalar(
        select(func.max(Publication.revision_number)).where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("denied", "expired", "succeeded", "failed")),
            Publication.outcome_history_ready_at.is_not(None),
        )
    )
    return int(revision or 0)


async def publication_lineage_has_inflight_push(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return whether an authorized revision may currently be changing GitHub."""

    if lineage.status != "open":
        return False
    inflight_id = await session.scalar(
        select(Publication.id)
        .where(
            Publication.lineage_id == lineage.id,
            Publication.status.in_(("approved", "launching", "running")),
        )
        .limit(1)
    )
    return inflight_id is not None


async def initialize_publication_lineage_head(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
    *,
    expected_version: int,
    head_sha: str,
) -> ThreadPublicationLineage:
    """CAS-initialize the unknown head of a migrated URL-only lineage."""

    changed = await session.execute(
        update(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.id == lineage.id,
            ThreadPublicationLineage.status == "open",
            ThreadPublicationLineage.version == expected_version,
            ThreadPublicationLineage.head_sha.is_(None),
            ThreadPublicationLineage.pr_number == lineage.pr_number,
            ThreadPublicationLineage.pr_url == lineage.pr_url,
        )
        .values(
            head_sha=head_sha,
            version=ThreadPublicationLineage.version + 1,
            updated_at=func.now(),
        )
        .returning(ThreadPublicationLineage.id)
    )
    if changed.scalar_one_or_none() is None:
        await session.rollback()
        current = await session.get(ThreadPublicationLineage, lineage.id)
        if (
            current is not None
            and current.status == "open"
            and current.head_sha == head_sha
            and current.pr_number == lineage.pr_number
            and current.pr_url == lineage.pr_url
        ):
            return current
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage changed while its migrated head was initialized",
        )
    await session.commit()
    refreshed = await session.get(ThreadPublicationLineage, lineage.id)
    assert refreshed is not None
    await session.refresh(refreshed)
    return refreshed


async def mark_publication_lineage_terminal(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
    *,
    expected_version: int,
    expected_head_sha: str,
    state: str,
) -> ThreadPublicationLineage:
    """Persist observed terminal GitHub truth without changing the known head."""

    if state not in ("merged", "closed"):
        raise ValueError("terminal publication lineage state is invalid")
    changed = await session.execute(
        update(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.id == lineage.id,
            ThreadPublicationLineage.status == "open",
            ThreadPublicationLineage.version == expected_version,
            ThreadPublicationLineage.head_sha == expected_head_sha,
            ThreadPublicationLineage.pr_number == lineage.pr_number,
            ThreadPublicationLineage.pr_url == lineage.pr_url,
        )
        .values(
            status=state,
            version=ThreadPublicationLineage.version + 1,
            updated_at=func.now(),
        )
        .returning(ThreadPublicationLineage.id)
    )
    if changed.scalar_one_or_none() is None:
        await session.rollback()
        current = await session.get(ThreadPublicationLineage, lineage.id)
        if (
            current is not None
            and current.status == state
            and current.head_sha == expected_head_sha
            and current.pr_number == lineage.pr_number
            and current.pr_url == lineage.pr_url
        ):
            return current
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage changed while its terminal state was refreshed",
        )
    await session.commit()
    refreshed = await session.get(ThreadPublicationLineage, lineage.id)
    assert refreshed is not None
    await session.refresh(refreshed)
    return refreshed


async def advance_publication_lineage(
    session: AsyncSession,
    publication_id: uuid.UUID,
    data: PublicationLineageAdvance,
    *,
    identity: VerifiedPublicationIdentity | None = None,
) -> ThreadPublicationLineage:
    """Atomically advance one approved revision and its exact lineage head."""

    publication = await session.scalar(
        select(Publication).where(Publication.id == publication_id).with_for_update()
    )
    if publication is None:
        raise LookupError("publication not found")
    if publication.lineage_id is None:
        raise PublicationLineageConflict(
            "publication.lineage_absent",
            "publication has no thread pull request lineage",
        )
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == publication.lineage_id)
        .with_for_update()
    )
    if lineage is None:
        raise PublicationLineageConflict(
            "publication.lineage_absent",
            "publication thread pull request lineage is absent",
        )
    if lineage.status != "open":
        raise PublicationLineageConflict(
            "publication.lineage_terminal",
            "the pull request for this thread is merged or closed; start a new thread",
        )
    if publication.revision_number != lineage.latest_revision:
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "publication revision is not the current thread lineage revision",
        )
    if data.pr_url != f"https://github.com/{lineage.repo_full_name}/pull/{data.pr_number}":
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request identity does not match the publication repository",
        )
    if lineage.pr_number is not None and (
        lineage.pr_number != data.pr_number or lineage.pr_url != data.pr_url
    ):
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request identity no longer matches the stored thread lineage",
        )
    if lineage.version != data.expected_version or lineage.head_sha != data.expected_head_sha:
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage version or expected head is stale",
        )
    if publication.status not in ("approved", "launching", "running"):
        raise PublicationLineageConflict(
            "publication.revision_not_approved",
            "publication revision must be approved before advancing its lineage",
        )

    identity_values: dict[str, Any] = {}
    if identity is not None:
        if lineage.github_repository_id is None:
            if lineage.pr_number is not None or lineage.binding_id is None:
                raise PublicationLineageConflict(
                    "publication.review_ineligible",
                    "historical lineage identity cannot be reconstructed",
                )
        elif (
            lineage.github_repository_id,
            lineage.github_installation_id,
            lineage.github_pr_node_id,
            lineage.base_ref,
        ) != (
            identity.repository_id,
            identity.installation_id,
            identity.pr_node_id,
            identity.base_ref,
        ):
            raise PublicationLineageConflict(
                "publication.lineage_stale", "immutable GitHub lineage identity changed"
            )
        binding = (
            await session.get(AgentChannel, lineage.binding_id, with_for_update=True)
            if lineage.binding_id is not None
            else None
        )
        if (
            binding is None
            or binding.agent_id != lineage.agent_id
            or binding.generation != lineage.binding_generation
        ):
            raise PublicationLineageConflict(
                "publication.lineage_stale", "original publication binding changed"
            )
        identity_values = {
            "github_repository_id": identity.repository_id,
            "github_installation_id": identity.installation_id,
            "github_pr_node_id": identity.pr_node_id,
            "base_ref": identity.base_ref,
        }
    elif lineage.github_repository_id is not None:
        raise PublicationLineageConflict(
            "publication.lineage_stale", "verified lineage advance requires current GitHub identity"
        )

    head_predicate = (
        ThreadPublicationLineage.head_sha.is_(None)
        if data.expected_head_sha is None
        else ThreadPublicationLineage.head_sha == data.expected_head_sha
    )
    changed = await session.execute(
        update(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.id == lineage.id,
            ThreadPublicationLineage.status == "open",
            ThreadPublicationLineage.version == data.expected_version,
            head_predicate,
        )
        .values(
            **identity_values,
            pr_number=data.pr_number,
            pr_url=data.pr_url,
            head_sha=data.head_sha,
            status=data.state,
            version=ThreadPublicationLineage.version + 1,
            updated_at=func.now(),
        )
        .returning(ThreadPublicationLineage.id)
    )
    if changed.scalar_one_or_none() is None:
        await session.rollback()
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage changed before this revision could advance it",
        )

    terminal_state = data.state in ("merged", "closed")
    publication_status = "failed" if terminal_state else "succeeded"
    publication_values: dict[str, Any] = {
        "status": publication_status,
        "version": Publication.version + 1,
        "patch_bytes": None,
        "terminal_at": func.now(),
        "updated_at": func.now(),
        "result_url": data.pr_url,
    }
    if terminal_state:
        publication_values["error"] = (
            "the pull request for this thread is merged or closed; start a new thread"
        )
    settled = await session.execute(
        update(Publication)
        .where(
            Publication.id == publication.id,
            Publication.status == publication.status,
            Publication.version == publication.version,
        )
        .values(**publication_values)
        .returning(Publication.id)
    )
    if settled.scalar_one_or_none() is None:
        await session.rollback()
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "publication revision changed before its lineage could advance",
        )
    await session.commit()
    refreshed = await session.get(ThreadPublicationLineage, lineage.id)
    assert refreshed is not None
    await session.refresh(refreshed)
    return refreshed


async def append_credential_redemption_audit(
    session: AsyncSession,
    *,
    purpose: str,
    outcome: str,
    deployment_id: uuid.UUID | None,
    publication_id: uuid.UUID | None,
    repo_full_name: str | None,
    detail: str | None,
) -> None:
    session.add(
        CredentialRedemptionAuditEntry(
            purpose=purpose,
            outcome=outcome,
            deployment_id=deployment_id,
            publication_id=publication_id,
            repo_full_name=repo_full_name,
            detail=detail,
        )
    )
    await session.commit()


async def reap_terminal_publication_patches(
    session: AsyncSession, *, terminal_before: datetime, limit: int
) -> int:
    ids = list(
        await session.scalars(
            select(Publication.id)
            .where(
                Publication.status.in_(("denied", "expired", "succeeded", "failed")),
                Publication.terminal_at.is_not(None),
                Publication.terminal_at <= terminal_before,
                Publication.patch_bytes.is_not(None),
            )
            .order_by(Publication.terminal_at)
            .limit(limit)
        )
    )
    if not ids:
        return 0
    await session.execute(
        update(Publication)
        .where(Publication.id.in_(ids))
        .values(patch_bytes=None, updated_at=func.now())
    )
    await session.commit()
    return len(ids)


async def create_action(session: AsyncSession, data: ActionRecord) -> AgentAction:
    """Insert a pending action record.

    Raises IntegrityError on a ``dedupe_key`` replay; the router maps that to the
    existing record, so a redelivered turn adopts what it already wrote.
    """

    action = AgentAction(
        agent_id=data.agent_id,
        conversation_id=data.conversation_id,
        call_id=data.call_id,
        tool=data.tool,
        arguments=data.arguments,
        detail=data.detail,
        gate_approval_id=data.gate_approval_id,
        dedupe_key=data.dedupe_key,
        status=ActionStatus.pending,
    )
    session.add(action)
    await session.commit()
    await session.refresh(action)
    return action


async def get_action(session: AsyncSession, action_id: uuid.UUID) -> AgentAction | None:
    return await session.get(AgentAction, action_id)


async def get_action_by_dedupe_key(session: AsyncSession, key: str) -> AgentAction | None:
    result = await session.execute(select(AgentAction).where(AgentAction.dedupe_key == key))
    return result.scalar_one_or_none()


async def list_actions(
    session: AsyncSession,
    *,
    conversation_id: str | None = None,
    agent_id: uuid.UUID | None = None,
    limit: int = 50,
) -> list[AgentAction]:
    """A conversation's actions, oldest first -- the order a receipt lists them."""

    query = select(AgentAction)
    if conversation_id is not None:
        query = query.where(AgentAction.conversation_id == conversation_id)
    if agent_id is not None:
        query = query.where(AgentAction.agent_id == agent_id)
    query = query.order_by(AgentAction.created_at, AgentAction.call_id).limit(limit)
    result = await session.execute(query)
    return list(result.scalars().all())


async def complete_action(
    session: AsyncSession, action: AgentAction, data: ActionComplete
) -> AgentAction:
    """Record what came back, once.

    A completion that arrives for an already-completed record is a redelivery,
    not a correction: the first account of a call is the one that was true when
    it happened, and overwriting it with a second would silently move a prior
    state a restore is about to replay. Returned unchanged.
    """

    if action.status != ActionStatus.pending:
        return action
    action.status = ActionStatus.failed if data.failed else ActionStatus.succeeded
    action.result = data.result
    action.prior_state = data.prior_state
    action.post_state = data.post_state
    action.target = data.target
    if data.detail is not None:
        action.detail = data.detail
    action.completed_at = datetime.now(UTC).replace(tzinfo=None)
    await session.commit()
    await session.refresh(action)
    return action


async def list_action_audit(session: AsyncSession, action_id: uuid.UUID) -> list[ActionAuditEntry]:
    result = await session.execute(
        select(ActionAuditEntry)
        .where(ActionAuditEntry.action_id == action_id)
        .order_by(ActionAuditEntry.created_at)
    )
    return list(result.scalars().all())


async def claim_action_undo(
    session: AsyncSession, action: AgentAction, *, actor: str
) -> AgentAction:
    """Mark the undo claimed so a second ruling cannot authorize a second restore.

    Claimed at ruling time rather than on completion, because nothing reports
    completion yet: the executor ADR-0117 leaves undecided is what would. The
    honest consequence is that a restore which never runs leaves a record saying
    it was, and closing that is the executor's job -- authorizing two restores of
    one action is the worse failure of the two.
    """

    action.undone_at = datetime.now(UTC).replace(tzinfo=None)
    action.undone_by = actor
    session.add(action)
    return action


async def create_approval(
    session: AsyncSession,
    data: "ApprovalRequest",
    *,
    traceparent: str | None = None,
) -> Approval:
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
        traceparent=traceparent,
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

    None still covers every legitimate miss -- a generic approval with no agent
    (``agent_id`` is nullable by design), an approval with no route, an agent
    with no bindings, a route the map does not bind -- but it no longer means
    one thing. ADR-0123 makes the selector split a None on whether the approval
    NAMED a route: a routeless approval keeps the AC4 zero-setup channel
    membership, while a routed approval with no binding is refused outright,
    because a route the operator narrowed must not be readable as one they never
    narrowed.

    This function deliberately still returns a bare None and does not say which
    of the four misses happened. The selector needs only ``approval.route`` to
    make that split, and a richer return type is not something ADR-0123 asks
    for. Note the guard below is ``not approval.route``, so a ``route=""``
    approval is routeless here; the selector keys on the same truthiness so the
    two files cannot disagree about what "named a route" means.

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


async def pending_approval_inventory(
    session: AsyncSession,
) -> tuple[int, datetime | None]:
    """Fleet-wide pending count and oldest creation time, without pagination."""

    count, oldest = (
        await session.execute(
            select(func.count(Approval.id), func.min(Approval.created_at)).where(
                Approval.status == ApprovalStatus.pending
            )
        )
    ).one()
    return int(count), oldest


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

    values: dict[str, Any] = {
        "status": decision,
        "resolved_by": resolved_by,
        "resolution_note": note,
        "resolved_at": func.now(),
    }
    # Publication outcomes are reported by the platform worker, never by a
    # resumed model turn. Mark the approval as owing no wake in the same CAS.
    publication = await get_publication_by_approval(session, approval_id)
    if publication is not None:
        values["resumed_at"] = func.now()

    result = await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.status == ApprovalStatus.pending)
        .values(**values)
        .returning(Approval.id)
    )
    claimed = result.scalar_one_or_none()
    if claimed is not None and publication is not None:
        publication_status = "approved" if decision == ApprovalStatus.approved else "denied"
        publication_values: dict[str, Any] = {
            "status": publication_status,
            "version": Publication.version + 1,
            "updated_at": func.now(),
        }
        if publication_status == "denied":
            publication_values["terminal_at"] = func.now()
            publication_values["patch_bytes"] = None
        changed = await session.execute(
            update(Publication)
            .where(
                Publication.id == publication.id,
                Publication.status == "pending",
                Publication.version == publication.version,
            )
            .values(**publication_values)
            .returning(Publication.id)
        )
        if changed.scalar_one_or_none() is None:
            await session.rollback()
            return None
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

    publication = await get_publication_by_approval(session, approval_id)
    approval_values: dict[str, Any] = {
        "status": ApprovalStatus.expired,
        "resolved_at": func.now(),
    }
    if publication is not None:
        approval_values["resumed_at"] = func.now()
    result = await session.execute(
        update(Approval)
        .where(Approval.id == approval_id, Approval.status == ApprovalStatus.pending)
        .values(**approval_values)
        .returning(Approval.id)
    )
    claimed = result.scalar_one_or_none()
    if claimed is not None and publication is not None:
        await session.execute(
            update(Publication)
            .where(Publication.id == publication.id, Publication.status == "pending")
            .values(
                status="expired",
                patch_bytes=None,
                version=Publication.version + 1,
                updated_at=func.now(),
                terminal_at=func.now(),
            )
        )
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
            Approval.purpose != "publication",
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
            Approval.purpose != "publication",
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
            Approval.purpose != "publication",
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
    principal_kind: str | None = None,
    authenticated: bool = False,
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
        principal_kind=principal_kind,
        authenticated=authenticated,
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
    session: AsyncSession, *, subject: str, now: datetime | None = None
) -> tuple[str, ConsoleSession]:
    """Mint a login code and its pending session row.

    Args:
        session: The database session.
        subject: The administrator-selected identity this session will carry.
        now: Injectable clock, so expiry is testable without sleeping.

    Returns:
        ``(plaintext code, row)``. The plaintext is returned ONCE and never
        stored; only its hash is persisted.
    """
    if not subject.strip():
        raise ValueError("console session subject must not be blank")
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    code = new_login_code()
    row = ConsoleSession(
        subject=subject,
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

    "Live" means exchanged, unrevoked and unexpired. ADR-0106 consumes this
    store directly for console approval principals without widening platform
    API-key authentication; revocation and expiry therefore take effect on the
    next resolve attempt.

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


async def _require_review_binding(
    session: AsyncSession, lineage: ThreadPublicationLineage
) -> AgentChannel:
    """Recheck original authority under the same transaction as reservation use."""
    binding = (
        await session.get(
            AgentChannel, lineage.binding_id, with_for_update=True, populate_existing=True
        )
        if lineage.binding_id
        else None
    )
    workspace = await get_thread_workspace(
        session, agent_id=lineage.agent_id, conversation_id=lineage.conversation_id
    )
    deployment = await session.get(
        Deployment, lineage.deployment_id, with_for_update=True, populate_existing=True
    )
    if (
        binding is None
        or binding.agent_id != lineage.agent_id
        or binding.generation != lineage.binding_generation
        or not lineage.reply_conversation_id
        or scoped_conversation_id(binding.kind, binding.address, lineage.reply_conversation_id)
        != lineage.conversation_id
        or workspace is None
        or workspace.repo_full_name.casefold() != lineage.repo_full_name.casefold()
        or not repository_is_allowed(lineage.repo_full_name, get_settings().github_repo_allowlist)
        or deployment is None
        or deployment.agent_id != lineage.agent_id
        or deployment.status != "active"
    ):
        raise PublicationLineageConflict(
            "publication.review_ineligible",
            "original review binding or workspace is no longer authorized",
        )
    return binding


async def reserve_review_revision(
    session: AsyncSession,
    data: ReviewRevisionReserve,
) -> tuple[PublicationReviewReservation, ThreadPublicationLineage, bool]:
    """Reserve in the caller's transaction, so feedback insertion can be atomic.

    No commit, approval, queue entry, or GitHub write occurs here. A reservation
    is consumed only by PublicationCreate naming its exact accepted origin.
    """
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(
            ThreadPublicationLineage.github_repository_id == data.repository_id,
            ThreadPublicationLineage.pr_number == data.pr_number,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        lineage is None
        or lineage.status != "open"
        or lineage.head_sha is None
        or lineage.github_installation_id is None
        or lineage.github_pr_node_id is None
    ):
        raise PublicationLineageConflict(
            "publication.review_ineligible",
            "no verified open lineage owns this GitHub pull request",
        )
    binding = await _require_review_binding(session, lineage)
    existing = await session.scalar(
        select(PublicationReviewReservation)
        .where(PublicationReviewReservation.origin_key == data.origin_key)
        .with_for_update()
    )
    if existing is not None:
        if (
            existing.lineage_id != lineage.id
            or existing.lineage_version != data.expected_lineage_version
        ):
            raise PublicationLineageConflict(
                "publication.revision_conflict",
                "review origin was replayed with different lineage facts",
            )
        return existing, lineage, False
    if lineage.version != data.expected_lineage_version:
        raise PublicationLineageConflict(
            "publication.lineage_stale", "review expected a stale lineage version"
        )
    if await publication_lineage_has_pending_revision(
        session, lineage
    ) or await publication_lineage_has_pending_outcome(session, lineage):
        raise PublicationLineageConflict(
            "publication.revision_conflict",
            "a revision or its durable outcome already owns this lineage",
        )
    row = PublicationReviewReservation(
        id=uuid.uuid4(),
        origin_key=data.origin_key,
        lineage_id=lineage.id,
        lineage_version=lineage.version,
        expected_head_sha=lineage.head_sha,
        revision_number=lineage.latest_revision + 1,
        binding_id=binding.id,
        binding_generation=binding.generation,
        status="reserved",
        version=1,
    )
    session.add(row)
    try:
        await session.flush()
    except IntegrityError:
        # Caller owns rollback, including its feedback insertion. A reused origin
        # on a different PR cannot be adopted by this transaction.
        raise PublicationLineageConflict(
            "publication.revision_conflict", "review origin is already reserved"
        ) from None
    return row, lineage, True


async def cancel_review_revision(
    session: AsyncSession,
    reservation_id: uuid.UUID,
    *,
    origin_key: str,
    expected_version: int,
) -> PublicationReviewReservation:
    row = await session.scalar(
        select(PublicationReviewReservation)
        .where(PublicationReviewReservation.id == reservation_id)
        .with_for_update()
    )
    if row is None:
        raise LookupError("review reservation not found")
    if row.origin_key != origin_key or row.version != expected_version or row.status != "reserved":
        raise PublicationLineageConflict(
            "publication.revision_conflict", "review reservation changed before cancellation"
        )
    row.status = "cancelled"
    row.version += 1
    row.updated_at = func.now()
    await session.flush()
    return row
