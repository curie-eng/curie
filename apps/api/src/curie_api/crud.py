"""Database access helpers for agents, versions, deployments, and approvals."""

import hashlib
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from aci_protocol.turn import SLACK_KIND, matching_routes, route_identity
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
    ExecutionRequest,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    ThreadWorkspace,
    WorkItem,
)
from .publication_authority import VerifiedPublicationIdentity
from .publication_policy import (
    PLATFORM_ACTOR,
    PLATFORM_AUTHORIZER,
    POLICY_AUTO,
    POLICY_IDENTITY,
    PublicationPolicyConflict,
    publication_branch_name,
    publication_row_prefix,
)
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
    SourceBindingConfig,
    VersionCreate,
)
from .workspace_policy import repository_is_allowed

_WORKSPACE_UNSET = object()


class AmbiguousRoute(RuntimeError):
    """`binding_for_route` matched more than one row for one `(kind, adapter,
    address)`.

    Unreachable while migration 0023's `agent_channels_kind_address_key`
    (UNIQUE kind, address) caps the pair at one row, so `matching_bindings`
    can never hand this function more than one. Raised rather than returned as `None`
    (which would read as "no binding", hiding the ambiguity) or resolved by
    picking `matches[0]` (which would silently serve a different agent's
    route than the caller asked for) -- the same failure mode
    `routers/agents.py:_binding_for` raises a 409 on for the identical
    question at the write path. A router that reaches this once the contract
    migration for ADR-0168 decision 3 (#3100) widens that constraint to the
    triple decides its own response; none has to catch it yet.
    """


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
        source_bindings=_stored_source_bindings(data.source_bindings),
        secrets=data.secrets,
        memory=data.memory,
        publication_policy=data.publication_policy,
        publication_draft=data.publication_draft,
        publication_branch_prefix=data.publication_branch_prefix,
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


async def version_has_active_deployment(session: AsyncSession, version_id: uuid.UUID) -> bool:
    """Whether an active deployment row ALREADY points at this version (#2436).

    The version-scoped sibling of ``agent_has_active_deployment``, and the
    condition that makes the approval-route gate on a bundle attachment
    conditional. An ordinary pre-deployment upload stays unrestricted, because
    the CLI's ``prepare_deploy`` uploads before ``curie <tier> approvals`` binds;
    an attachment onto a version the worker's resolve query can already boot
    (``curie_worker.binding`` joins ``deployments.status = 'active'`` to
    ``agent_versions.bundle_ref``) is the moment the bundle goes live, so it is
    gated like a deployment.
    """

    result = await session.scalar(
        select(Deployment.id)
        .where(Deployment.version_id == version_id, Deployment.status == "active")
        .limit(1)
    )
    return result is not None


async def delete_agent(session: AsyncSession, agent_id: uuid.UUID) -> None:
    # Remove child rows first, then the agent. Bulk deletes bypass the ORM
    # relationship cascade (which would emit an async lazy-load during flush) and
    # match the FK ondelete=CASCADE already declared on every child table. Bundle
    # objects in RustFS are intentionally left in place (out of scope).
    await session.execute(delete(AgentChannel).where(AgentChannel.agent_id == agent_id))
    await session.execute(delete(WorkItem).where(WorkItem.agent_id == agent_id))
    await session.execute(delete(Deployment).where(Deployment.agent_id == agent_id))
    await session.execute(delete(AgentVersion).where(AgentVersion.agent_id == agent_id))
    await session.execute(delete(Agent).where(Agent.id == agent_id))
    await session.commit()


async def lock_agent_bindings(session: AsyncSession, agent_id: uuid.UUID) -> list[AgentChannel]:
    """`SELECT ... FOR UPDATE` the agent's WHOLE binding set, in route order.

    Ordered by the route `(kind, adapter, address)` (ADR-0168 decision 3), so
    the lock order stays total if several identities ever share a pair.
    `Agent.channels` reads `(kind, address)`; the two orders agree while
    migration 0023's `agent_channels_kind_address_key` holds one row per pair.

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
        .order_by(AgentChannel.kind, AgentChannel.adapter, AgentChannel.address)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(result)


async def agent_id_for_route(
    session: AsyncSession, kind: str, adapter: str | None, address: str
) -> uuid.UUID | None:
    """Which agent holds this ROUTE -- `(kind, adapter, address)` -- if any.

    Named rather than inlined at its call sites: it answers the question a
    binding write's 409 has to answer accurately -- is the duplicate THIS
    agent's or another's -- and an inline `select` there reads as an incidental
    query the next reader deletes. Replaces `agent_id_for_pair` (ADR-0168
    decision 3): the pair alone no longer names the row uniquely once several
    identities can share one `(kind, address)`.

    Selects the rows on `(kind, address)` and narrows to `adapter`'s RESOLVED
    identity in PYTHON, because a SQL predicate on the raw `adapter` column
    would miss a legacy NULL -- `route_identity` is what turns that NULL into
    'default' for Slack, and there is no portable SQL equivalent to call
    without duplicating the identity rule in a second language. Migration
    0023's `agent_channels_kind_address_key` (UNIQUE kind, address) holds the
    candidate set to at most one row until the contract migration for
    ADR-0168 decision 3 (#3100) widens it to the triple; the loop is for when
    several identities can share a pair.
    """

    wanted = route_identity(kind, adapter)
    result = await session.execute(
        select(AgentChannel.agent_id, AgentChannel.adapter).where(
            AgentChannel.kind == kind, AgentChannel.address == address
        )
    )
    for owner_id, stored_adapter in result.all():
        if route_identity(kind, stored_adapter) == wanted:
            owner: uuid.UUID = owner_id
            return owner
    return None


async def agent_id_for_channel_pair(
    session: AsyncSession, kind: str, address: str
) -> uuid.UUID | None:
    """Which agent holds this PAIR, ignoring identity -- if any.

    A deliberately narrower question than `agent_id_for_route`: the database
    constraint is still `agent_channels_kind_address_key` (UNIQUE kind,
    address; migration 0023, until the contract migration for ADR-0168
    decision 3 (#3100) widens it to the triple), so this is the lookup that
    answers what the constraint actually enforces. `agent_id_for_route` can
    legitimately return `None` while this agent still holds the pair under a
    DIFFERENT identity -- a custom-transport binding a bare identity-form
    write collided with, for instance -- and `_raise_binding_conflict` falls
    back to this lookup so that case still names the right agent instead of
    reading as "another agent already bound". `add_agent_channel` asks it too,
    to recognize a re-POST of a pair this agent already holds as idempotent.

    Not the routing question: that is `agent_id_for_route`'s. This one exists
    only because the pair constraint has not widened yet.
    """

    owner: uuid.UUID | None = await session.scalar(
        select(AgentChannel.agent_id).where(
            AgentChannel.kind == kind, AgentChannel.address == address
        )
    )
    return owner


async def agent_holds_channel_pair(
    session: AsyncSession, agent_id: uuid.UUID, kind: str, address: str
) -> bool:
    """Does THIS agent hold a row on `(kind, address)`, under any identity?

    Not `agent_id_for_channel_pair`: that answers who (if anyone) holds the
    pair with one arbitrary row when several could match, which only holds
    today because `agent_channels_kind_address_key` (migration 0023) still
    caps the pair to one row; once the contract migration for ADR-0168
    decision 3 (#3100) widens it to the triple, two agents can hold the same
    pair under two identities, and picking an arbitrary row would answer the
    wrong agent's question. This asks the narrower thing every caller here
    actually needs -- filtered on `agent_id` in the query itself, so it stays
    correct on either side of that migration.
    """

    held = await session.scalar(
        select(AgentChannel.id)
        .where(
            AgentChannel.agent_id == agent_id,
            AgentChannel.kind == kind,
            AgentChannel.address == address,
        )
        .limit(1)
    )
    return held is not None


def matching_bindings(
    bindings: list[AgentChannel], kind: str, address: str, adapter: str | None
) -> list[AgentChannel]:
    """The rows in ``bindings`` that `(kind, address, adapter)` selects.

    A thin, name-preserving wrapper over `aci_protocol.turn.matching_routes`
    (ADR-0168 decision 3), the one matching rule shared by every reader that
    has to answer "is this the same route" -- whether it already holds the
    candidate rows (`routers/hooks.py`'s preloaded `agent.channels`,
    `routers/agents.py`'s locked per-agent set) or fetches them fresh
    (`binding_for_route`, below).
    """

    return matching_routes(bindings, kind, address, adapter)


async def binding_for_route(
    session: AsyncSession,
    kind: str,
    adapter: str | None,
    address: str,
    *,
    for_update: bool = False,
) -> AgentChannel | None:
    """The single binding ROW `(kind, adapter, address)` names, or None.

    The shared answer for every reader that needs the row itself rather than
    only its owning agent id -- the read-path counterpart to
    `agent_id_for_route`. Selects the PAIR in SQL
    (`agent_channels_kind_address_key` still holds the pair to one row, so
    at most one row can match) and narrows to `adapter`'s identity with
    `matching_bindings`, so this function and every in-memory caller of that
    function agree on what counts as the same route.

    Raises `AmbiguousRoute` rather than returning `matches[0]` if
    `matching_bindings` ever returns more than one row -- unreachable today
    for the reason above, but once the contract migration for ADR-0168
    decision 3 (#3100) widens the constraint to the triple this must fail
    loud, not silently serve whichever row sorted first.

    `for_update` takes the same row lock `lock_agent_bindings` and
    `mint_channel_token` already take, with `populate_existing` for the same
    reason: a caller already holding this row in its identity map (loaded
    for an earlier check) must see the fresh, locked version rather than a
    stale one from before a concurrent winner's commit.
    """

    stmt = select(AgentChannel).where(AgentChannel.kind == kind, AgentChannel.address == address)
    if for_update:
        # Locks every row on the PAIR, not only the one `adapter` will match
        # -- harmless while the pair holds at most one row, but once the
        # contract migration for ADR-0168 decision 3 (#3100) lets several
        # identities share a pair the lock must narrow by adapter in SQL, or
        # a mint for one identity needlessly blocks a concurrent mint for
        # another on the same pair.
        stmt = stmt.with_for_update().execution_options(populate_existing=True)
    rows = list(await session.scalars(stmt))
    matches = matching_bindings(rows, kind, address, adapter)
    if len(matches) > 1:
        raise AmbiguousRoute(
            f"{len(matches)} rows match kind {kind!r}, adapter {adapter!r}, address "
            f"{address!r}; pass a more specific adapter to name one"
        )
    return matches[0] if matches else None


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
    that case silently valid. `POST /channels/token` is the sibling bump: a
    remint increments the same counter so rotation revokes (#2379).

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
    endpoint_sent = "endpoint" in channel.model_fields_set
    adapter_sent = "adapter" in channel.model_fields_set
    if endpoint_sent:
        binding.endpoint = channel.endpoint
        binding.adapter = channel.adapter
    elif channel.kind == SLACK_KIND and adapter_sent:
        # ADR-0168 decision 3: a Slack PATCH may name a new
        # identity ALONE, with no endpoint (`ChannelBindingPatch
        # ._check_route_presence` already allows this shape -- Slack's
        # implicit transport carries no endpoint). The
        # `"endpoint" in model_fields_set` gate above predates the identity
        # form and would otherwise silently drop the one field this branch's
        # caller actually sent.
        #
        # `endpoint` is cleared too, not left as it was: naming an identity
        # alone means moving TO the identity form (`ChannelBindingPatch`'s own
        # docstring), and 0024's `agent_channels_route_pair_ck` CHECKs
        # `(endpoint IS NULL) = (adapter IS NULL)` at the database -- writing
        # `channel.adapter` (None, the stored form of the default identity)
        # while a stale endpoint from a prior custom-transport row survives
        # would violate it. A row that WAS on the custom-transport form
        # therefore loses that transport on this move, which is the point:
        # the caller asked to be addressed by identity, not by a lingering URL.
        binding.adapter = channel.adapter
        binding.endpoint = None
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


async def update_agent_execution_deadline(
    session: AsyncSession, agent: Agent, seconds: int | None
) -> Agent:
    agent.execution_deadline_seconds = seconds
    await session.commit()
    await session.refresh(agent)
    return agent


async def update_agent_publication_policy(
    session: AsyncSession,
    agent: Agent,
    *,
    policy: str | None,
    draft: bool | None,
    branch_prefix: str | None,
    prefix_sent: bool,
) -> Agent:
    """Apply one operator publication-policy write and bump the version once.

    A request that names a field but does not change its stored value does not
    bump the version, so a repeated PATCH cannot revoke an in-flight approval.
    """

    values: dict[str, Any] = {}
    if policy is not None and policy != agent.publication_policy:
        values["publication_policy"] = policy
    if draft is not None and draft != agent.publication_draft:
        values["publication_draft"] = draft
    if prefix_sent and branch_prefix != agent.publication_branch_prefix:
        values["publication_branch_prefix"] = branch_prefix
    if not values:
        return agent
    expected_version = agent.publication_policy_version
    values["publication_policy_version"] = expected_version + 1
    # The version predicate is the compare-and-set. Two writers that read the
    # same version cannot both commit, so a publication created under the
    # winning version is revoked when the loser retries and bumps again.
    updated_version = await session.scalar(
        update(Agent)
        .where(
            Agent.id == agent.id,
            Agent.publication_policy_version == expected_version,
        )
        .values(**values)
        .returning(Agent.publication_policy_version)
    )
    if updated_version is None:
        await session.rollback()
        raise PublicationPolicyConflict("publication policy version changed; retry the read")
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


def _stored_source_bindings(
    bindings: dict[str, SourceBindingConfig] | None,
) -> dict[str, Any] | None:
    if not bindings:
        return None
    return {name: c.model_dump() for name, c in bindings.items()}


async def update_agent_source_bindings(
    session: AsyncSession, agent: Agent, bindings: dict[str, SourceBindingConfig]
) -> Agent:
    """Set the agent's workload-to-repository map (#2572). An empty dict clears it."""

    agent.source_bindings = _stored_source_bindings(bindings)
    await session.commit()
    await session.refresh(agent)
    return agent


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
    deployment_id: uuid.UUID | None,
    conversation_id: str,
    repo_full_name: str,
    selected_by: str,
    revision: str | None = None,
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
            revision=revision,
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


async def list_active_deployment_versions(
    session: AsyncSession, agent_id: uuid.UUID
) -> list[AgentVersion]:
    """Every DISTINCT version the agent has an active deployment of, one query.

    Deliberately NOT built on ``get_active_deployment`` (#2436). That helper
    returns only the NEWEST active row per environment, which is the right answer
    to "what is current" and the wrong set for "what can this write strand":
    git-flow appends a new active row per push without superseding older ones,
    ``end_deployment`` marks exactly one row stopped, and the worker's resolve
    query orders over ALL active rows
    (``apps/worker/src/curie_worker/binding.py``). So several active rows
    routinely coexist in one environment, and a check built on "newest per
    environment" would let an operator end the newest deployment and immediately
    unbind a route an older, still-active, still-bootable row declares.

    Rows sharing a version share one bundle object, so the join collapses them
    here rather than leaving the caller to de-duplicate a row list: each version
    comes back once, ordered by the earliest active row pointing at it.

    ``get_active_deployment`` is left unchanged: ``create_deployment_row``'s
    ``workspace_enabled`` inheritance depends on its "newest" semantics.
    """

    result = await session.scalars(
        select(AgentVersion)
        .join(Deployment, Deployment.version_id == AgentVersion.id)
        .where(Deployment.agent_id == agent_id, Deployment.status == "active")
        .group_by(AgentVersion.id)
        .order_by(func.min(Deployment.deployed_at))
    )
    return list(result)


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


_ACTIVE_WORK_ITEM_STATUSES = ("waiting", "running", "cancellation_requested")


async def publication_cancellation_conflict(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
) -> PublicationLineageConflict | None:
    """Refuse credential redemption after the owning work item is cancelled.

    A running request may still publish. Cancellation of the work item, or an
    active request already in ``cancellation_requested``, may not.
    """

    work_item = await session.scalar(
        select(WorkItem)
        .where(
            WorkItem.agent_id == agent_id,
            WorkItem.conversation_id == conversation_id,
        )
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if work_item is None:
        return None
    if work_item.cancelled_at is not None:
        return PublicationLineageConflict(
            "publication.work_item_cancelled",
            "this conversation's work item is cancelled",
        )
    active = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.status == "cancellation_requested",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if active is None:
        return None
    return PublicationLineageConflict(
        "publication.work_item_cancelled",
        "this conversation's work item is cancelled",
    )


async def _refuse_fenced_work_item(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
    request_id: uuid.UUID | None,
    runtime_epoch: int | None,
) -> ExecutionRequest | None:
    work_item = await session.scalar(
        select(WorkItem)
        .where(
            WorkItem.agent_id == agent_id,
            WorkItem.conversation_id == conversation_id,
        )
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if work_item is None:
        return None
    if work_item.cancelled_at is not None:
        raise PublicationLineageConflict(
            "publication.work_item_cancelled",
            "this conversation's work item is cancelled",
        )
    active = await session.scalar(
        select(ExecutionRequest)
        .where(
            ExecutionRequest.work_item_id == work_item.id,
            ExecutionRequest.status.in_(_ACTIVE_WORK_ITEM_STATUSES),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if active is None:
        return None
    if active.status == "cancellation_requested":
        raise PublicationLineageConflict(
            "publication.work_item_cancelled",
            "this conversation's work item is cancelled",
        )
    # A resumed approval turn does not carry the execute event id. The only
    # running request for this conversation owns the publication.
    if request_id is None and runtime_epoch is None:
        return active
    if active.status == "running" and (
        request_id != active.id or runtime_epoch != active.runtime_epoch
    ):
        raise PublicationLineageConflict(
            "publication.work_item_stale_owner",
            "the publication is not owned by the running work item request",
        )
    return active


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
    agent = await get_agent(session, deployment.agent_id)
    if agent is None:
        raise LookupError("agent not found")
    auto = agent.publication_policy == POLICY_AUTO

    owned_request = await _refuse_fenced_work_item(
        session,
        agent_id=deployment.agent_id,
        conversation_id=workspace_conversation_id,
        request_id=data.work_item_request_id,
        runtime_epoch=data.work_item_runtime_epoch,
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
                "publication.review_ineligible",
                "review origin has no existing lineage",
            )
        binding = await session.scalar(
            select(AgentChannel)
            .where(
                AgentChannel.agent_id == deployment.agent_id,
                AgentChannel.kind == data.reply_kind,
                AgentChannel.address == data.reply_channel,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if binding is not None and (
            binding.endpoint != data.reply_endpoint
            or route_identity(data.reply_kind, binding.adapter)
            != route_identity(data.reply_kind, data.reply_adapter)
        ):
            # Compared through `route_identity`, not the raw column (ADR-0168
            # decision 3): a Slack binding is stored with `adapter=None` and a
            # wire-side `reply_adapter='default'` names the SAME identity; a
            # raw `!=` would see a mismatch and drop a lineage this
            # publication should adopt.
            binding = None
        lineage_id = uuid.uuid4()
        lineage = ThreadPublicationLineage(
            id=lineage_id,
            agent_id=deployment.agent_id,
            deployment_id=deployment.id,
            conversation_id=workspace_conversation_id,
            repo_full_name=thread_workspace.repo_full_name,
            base_sha=data.base_sha,
            branch=publication_branch_name(
                lineage_id.hex,
                prefix=agent.publication_branch_prefix,
                auto=auto,
            ),
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
            .execution_options(populate_existing=True)
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
                # Through `route_identity`, not the raw column (ADR-0168
                # decision 3): a stored `adapter=None` Slack binding and a
                # wire-side `reply_adapter='default'` name the same identity,
                # so comparing the raw columns would refuse a review revision
                # replaying the exact route its own reservation was raised on.
                or route_identity(data.reply_kind, data.reply_adapter)
                != route_identity(review_binding.kind, review_binding.adapter)
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
            reservation.updated_at = func.now()
        elif data.review_origin_key is not None:
            raise PublicationLineageConflict(
                "publication.review_ineligible",
                "review origin has no active reservation",
            )
        else:
            revision_number = lineage.latest_revision + 1
        lineage.latest_revision = revision_number
        lineage.updated_at = func.now()

    if (
        auto
        and agent.publication_branch_prefix
        and not lineage.branch.startswith(agent.publication_branch_prefix)
    ):
        raise PublicationLineageConflict(
            "publication.branch_prefix",
            "the publication branch does not carry the operator branch prefix",
        )

    expected_prior_head = lineage.head_sha or lineage.base_sha
    expires_at = None
    if data.expires_in_seconds is not None:
        expires_at = datetime.now(UTC).replace(tzinfo=None) + timedelta(
            seconds=data.expires_in_seconds
        )
    resolved_at = datetime.now(UTC).replace(tzinfo=None) if auto else None
    approval = Approval(
        id=uuid.uuid4(),
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
        route=data.route,
        card_channel=data.reply_channel,
        gate_kind="permission",
        granted_tool="mcp__curie__publish_changes",
        purpose="publication",
        expires_at=expires_at,
        status=ApprovalStatus.approved if auto else ApprovalStatus.pending,
        resolved_by=PLATFORM_ACTOR if auto else None,
        resolution_note=(
            f"authorized by {POLICY_IDENTITY} version {agent.publication_policy_version}"
            if auto
            else None
        ),
        resolved_at=resolved_at,
        resumed_at=resolved_at,
        policy_identity=POLICY_IDENTITY if auto else None,
        policy_version=agent.publication_policy_version if auto else None,
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
        status="approved" if auto else "pending",
        open_as_draft=bool(auto and agent.publication_draft),
        branch_prefix=publication_row_prefix(
            lineage.branch,
            operator_prefix=agent.publication_branch_prefix,
            auto=auto,
        ),
        approval_card_reported_at=resolved_at,
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
    if owned_request is not None:
        publication.execution_request_id = owned_request.id
    session.add(publication)
    await _bind_running_work_item_lineage(
        session,
        agent_id=deployment.agent_id,
        conversation_id=workspace_conversation_id,
        lineage_id=lineage.id,
    )
    if auto:
        session.add(
            ApprovalAuditEntry(
                approval_id=approval.id,
                action="resolved",
                actor=PLATFORM_ACTOR,
                actor_channel=None,
                principal_kind="platform",
                authenticated=True,
                decision=ApprovalStatus.approved,
                authorizer=PLATFORM_AUTHORIZER,
                authorized=True,
                reason=approval.resolution_note,
                evidence={
                    "policy_identity": POLICY_IDENTITY,
                    "policy_version": agent.publication_policy_version,
                    "agent_id": str(agent.id),
                    "publication_draft": bool(agent.publication_draft),
                    "publication_branch_prefix": agent.publication_branch_prefix,
                },
            )
        )
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


async def _bind_running_work_item_lineage(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    conversation_id: str,
    lineage_id: uuid.UUID,
) -> None:
    """Point the running factory request at this publication before commit.

    The publication transaction already holds the work item. A conversation
    with no running request is left alone.
    """

    await session.execute(
        update(WorkItem)
        .where(
            WorkItem.agent_id == agent_id,
            WorkItem.conversation_id == conversation_id,
            WorkItem.cancelled_at.is_(None),
            WorkItem.publication_lineage_id.is_(None),
            select(ExecutionRequest.id)
            .where(
                ExecutionRequest.work_item_id == WorkItem.id,
                ExecutionRequest.status == "running",
                ExecutionRequest.execution_deadline > func.clock_timestamp(),
            )
            .exists(),
        )
        .values(
            publication_lineage_id=lineage_id,
            version=WorkItem.version + 1,
            updated_at=func.clock_timestamp(),
        )
    )


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
        statement = statement.with_for_update().execution_options(populate_existing=True)
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
    """Return whether an active publication revision owns this lineage."""

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
    return pending_id is not None


async def _publication_lineage_has_reserved_review(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
) -> bool:
    """Return whether a verified review currently reserves this lineage."""

    if lineage.status != "open":
        return False
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


def publication_lineage_outcome_conflict(
    publication: Publication,
    lineage: ThreadPublicationLineage,
    data: PublicationLineageAdvance,
) -> PublicationLineageConflict | None:
    """Preconditions one revision outcome must meet before it may claim a lineage.

    Pure, so the route can reject a stale outcome before it contacts GitHub and
    the advancing writer can repeat the same verdict under its row locks. The
    order is load bearing and carried over unchanged. Which check fires first
    decides which conflict code the caller sees, and the worker branches on it.
    """

    if lineage.status != "open":
        return PublicationLineageConflict(
            "publication.lineage_terminal",
            "the pull request for this thread is merged or closed; start a new thread",
        )
    if publication.revision_number != lineage.latest_revision:
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "publication revision is not the current thread lineage revision",
        )
    # Case insensitive on both sides: `_validated_pr_url` in the worker accepts
    # GitHub's own spelling of the repository and preserves it, so a repository
    # whose GitHub casing differs from `repo_full_name` publishes fine and must
    # not then take a stable refusal here.
    canonical = f"https://github.com/{lineage.repo_full_name}/pull/{data.pr_number}"
    if data.pr_url.casefold() != canonical.casefold():
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request identity does not match the publication repository",
        )
    if lineage.pr_number is not None and (
        lineage.pr_number != data.pr_number
        or (lineage.pr_url or "").casefold() != data.pr_url.casefold()
    ):
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request identity no longer matches the stored thread lineage",
        )
    if lineage.version != data.expected_version or lineage.head_sha != data.expected_head_sha:
        return PublicationLineageConflict(
            "publication.lineage_stale",
            "pull request lineage version or expected head is stale",
        )
    if (
        publication.version != data.expected_publication_version
        or publication.lease_owner != data.lease_owner
    ):
        return PublicationLineageConflict(
            "publication.lease_lost",
            "publication lease is no longer held by this worker",
        )
    if publication.status not in ("approved", "launching", "running"):
        return PublicationLineageConflict(
            "publication.revision_not_approved",
            "publication revision must be approved before advancing its lineage",
        )
    return None


async def advance_publication_lineage(
    session: AsyncSession,
    publication_id: uuid.UUID,
    data: PublicationLineageAdvance,
    *,
    identity: VerifiedPublicationIdentity | None = None,
) -> ThreadPublicationLineage:
    """Atomically advance one approved revision and its exact lineage head."""

    publication = await session.scalar(
        select(Publication)
        .where(Publication.id == publication_id)
        .with_for_update()
        .execution_options(populate_existing=True)
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
        .execution_options(populate_existing=True)
    )
    if lineage is None:
        raise PublicationLineageConflict(
            "publication.lineage_absent",
            "publication thread pull request lineage is absent",
        )
    conflict = publication_lineage_outcome_conflict(publication, lineage, data)
    if conflict is not None:
        raise conflict

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
                "publication.lineage_stale",
                "immutable GitHub lineage identity changed",
            )
        await require_current_lineage_workspace(
            session,
            lineage,
            conflict_code="publication.lineage_stale",
            conflict_message="publication workspace or deployment is no longer authorized",
        )
        identity_values = {
            "github_repository_id": identity.repository_id,
            "github_installation_id": identity.installation_id,
            "github_pr_node_id": identity.pr_node_id,
            "base_ref": identity.base_ref,
        }
    elif lineage.github_repository_id is not None:
        raise PublicationLineageConflict(
            "publication.lineage_stale",
            "verified lineage advance requires current GitHub identity",
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
        # Settle the worker's publication lease with the outcome, exactly as
        # its terminal CAS would, so the result outbox is claimable at once.
        "lease_owner": None,
        "lease_expires_at": None,
        "terminal_at": func.now(),
        "updated_at": func.now(),
        "result_url": data.pr_url,
        # Success replaces an earlier attempt's error, as the worker CAS did.
        "error": None,
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
            Publication.version == data.expected_publication_version,
            Publication.lease_owner == data.lease_owner,
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


# Per served agent: its approval route map, read fresh, and the (kind, address)
# pairs of the adapter's bindings that belong to that agent.
_ServedTargets = dict[uuid.UUID, tuple[Any, frozenset[tuple[str, str]]]]


async def _adapter_served_targets(
    session: AsyncSession, bindings: frozenset[uuid.UUID]
) -> _ServedTargets:
    """What an adapter serving ``bindings`` can reach, keyed by agent id.

    Read fresh on every call, like ``get_approval_route_binding``: a binding
    deleted or a route re-pointed after the credential was issued narrows what
    the adapter sees immediately.

    Holds a binding row of ANY kind, Slack included: an adapter principal
    (ADR-0154) is scoped to the binding ROW id its token claims, not to a
    kind that can authenticate HTTP egress, so a principal may legitimately
    serve a Slack binding (``test_adapter_principal.py``'s own default
    fixture is one).
    """

    if not bindings:
        return {}
    rows = await session.execute(
        select(
            AgentChannel.agent_id,
            AgentChannel.kind,
            AgentChannel.address,
            Agent.approval_routes,
        )
        .join(Agent, Agent.id == AgentChannel.agent_id)
        .where(AgentChannel.id.in_(bindings))
    )
    pairs: dict[uuid.UUID, set[tuple[str, str]]] = {}
    routes: dict[uuid.UUID, Any] = {}
    for agent_id, kind, address, approval_routes in rows:
        pairs.setdefault(agent_id, set()).add((kind, address))
        routes[agent_id] = approval_routes
    return {agent_id: (routes[agent_id], frozenset(p)) for agent_id, p in pairs.items()}


def _approval_served(approval: Approval, targets: _ServedTargets) -> bool:
    """THE served predicate (ADR-0154), shared by the list and the resolver.

    An approval is served when it names an agent and a route, and that agent's
    route resolves to the ``(kind, address)`` of one of the adapter's bindings
    ON THE SAME AGENT. A routeless approval, or a route whose resolution is
    missing or malformed, is served by no adapter: fail closed.
    """

    if approval.agent_id is None or not approval.route:
        return False
    target = targets.get(approval.agent_id)
    if target is None:
        return False
    approval_routes, pairs = target
    if not isinstance(approval_routes, dict):
        return False
    binding = approval_routes.get(approval.route)
    if not isinstance(binding, dict):
        return False
    resolution = binding.get("resolution")
    if not isinstance(resolution, dict):
        return False
    kind, address = resolution.get("kind"), resolution.get("address")
    if not isinstance(kind, str) or not isinstance(address, str):
        return False
    return (kind, address) in pairs


async def approval_served_by(
    session: AsyncSession, approval: Approval, bindings: frozenset[uuid.UUID]
) -> bool:
    """Whether an adapter serving ``bindings`` may see and resolve ``approval``."""

    return _approval_served(approval, await _adapter_served_targets(session, bindings))


async def existing_channel_binding_ids(
    session: AsyncSession, binding_ids: frozenset[uuid.UUID]
) -> frozenset[uuid.UUID]:
    """The subset of ``binding_ids`` that still name an ``agent_channels`` row."""

    if not binding_ids:
        return frozenset()
    result = await session.scalars(select(AgentChannel.id).where(AgentChannel.id.in_(binding_ids)))
    return frozenset(result)


async def list_approvals(
    session: AsyncSession,
    *,
    status: str | None = None,
    agent_id: uuid.UUID | None = None,
    conversation_id: str | None = None,
    limit: int = 50,
    served_by: frozenset[uuid.UUID] | None = None,
) -> list[Approval]:
    """Newest first. ``served_by`` (an adapter principal's bindings) narrows the
    result to approvals that adapter serves, BEFORE ``limit`` applies, so an
    adapter never gets a short page because unserved rows took the slots."""

    stmt = select(Approval).order_by(Approval.created_at.desc())
    if status is not None:
        stmt = stmt.where(Approval.status == status)
    if agent_id is not None:
        stmt = stmt.where(Approval.agent_id == agent_id)
    if conversation_id is not None:
        stmt = stmt.where(Approval.conversation_id == conversation_id)
    if served_by is None:
        result = await session.scalars(stmt.limit(limit))
        return list(result)
    targets = await _adapter_served_targets(session, served_by)
    if not targets:
        return []
    # Narrow in SQL to the served agents' routed rows, then apply the one
    # predicate the resolver also uses; the route map is JSONB, so the
    # resolution match itself stays in Python. The SQL side still needs its
    # own bound: `_approval_served` can only drop rows, never keep more than
    # it's given, so a hard cap here (well above `limit`) keeps a busy agent's
    # adapter listing from materializing every routed approval it has.
    stmt = stmt.where(Approval.agent_id.in_(targets), Approval.route.is_not(None)).limit(
        max(limit, 1000)
    )
    served = [a for a in await session.scalars(stmt) if _approval_served(a, targets)]
    return served[:limit]


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
    if (
        publication is not None
        and publication.execution_request_id is not None
        and decision == ApprovalStatus.approved
    ):
        owning = await session.get(ExecutionRequest, publication.execution_request_id)
        if owning is None or owning.status != "running":
            decision = ApprovalStatus.rejected
            values["status"] = decision
            values["resolution_note"] = "the factory run already ended"
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
    principal_subject: str | None = None,
) -> ApprovalAuditEntry:
    """Append one audit row (#247). Append-only by design; never updated.

    ``evidence`` (#420) is the membership snapshot the authorizer decided on;
    None for writers that made no membership decision. ``principal_subject``
    names the adapter that transported an ``adapter`` principal's decision
    (ADR-0154); None for every other kind.
    """

    entry = ApprovalAuditEntry(
        approval_id=approval_id,
        action=action,
        actor=actor,
        actor_channel=actor_channel,
        principal_kind=principal_kind,
        authenticated=authenticated,
        principal_subject=principal_subject,
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


# --- break-glass recovery (#2753) --------------------------------------------
#
# ONE transaction, ONE commit, per operation. ``claim_approval_resolution``
# commits internally, and so does ``append_approval_audit``; composing the two
# leaves a crash window in which the status flipped and the audit row that
# explains it never existed. For a path whose whole justification is that every
# use is reviewable afterwards, that window is the failure, so recovery does
# the compare-and-set and the audit append inside a single ``session.begin()``
# block instead of calling either.
#
# Idempotency is keyed on that audit row, not on a column: the caller's
# ``recovery_key`` is recorded in the row's evidence, and because the row
# commits with the CAS, a key with no row means no effect landed.
#
# The audit row is built from the module-level ``ApprovalAuditEntry``, exactly
# as ``append_approval_audit`` does. That is deliberate and load-bearing: the
# seam between the CAS and the audit append has to be the same one the existing
# writer exposes, so a test can interrupt precisely there. A Core ``insert()``
# here would move the seam.

#: Everything the caller must supply about WHO acted. Recovery takes its actor
#: from the ADR-0106 operator principal for attribution only; no membership is
#: consulted and nothing widens.
_RECOVERY_AUTHORIZER = "approval_recovery"


class PublicationSettlementConflict(Exception):
    """The recovered approval's publication moved under the recovery.

    Raised INSIDE the recovery transaction so the whole administrative act --
    the approval CAS, the publication settlement and the audit row -- rolls back
    together. A recovery that settled the approval and left the publication
    pending would be exactly the stranded effect this path exists to remove.
    """


async def reread_approval(session: AsyncSession, approval_id: uuid.UUID) -> Approval | None:
    """Read an approval back from the database, not from the identity map.

    An ORM-enabled Core UPDATE expires the columns it touched on any instance
    already in the session, so a plain ``session.get`` hands back an object
    whose next attribute access is a lazy load -- which under the async session
    is a ``MissingGreenlet``, not a refresh. ``claim_approval_resolution``
    refreshes for the same reason.
    """

    approval = await session.get(Approval, approval_id)
    if approval is not None:
        await session.refresh(approval)
    return approval


#: The audit model as the replay lookup reads it. A separate name on purpose:
#: ``recover_approval_atomic`` builds its row from the module-level
#: ``ApprovalAuditEntry`` so a test can interrupt exactly between the CAS and
#: the audit append, and the lookup must not be caught by that interruption.
_RecoveryAuditEntry = ApprovalAuditEntry

#: The audit action every administrative recovery writes. Its evidence carries
#: the caller's ``recovery_key``, which is what a replay is matched on.
RECOVERY_AUDIT_ACTION = "administratively_recovered"


async def find_recovery_audit(
    session: AsyncSession, recovery_key: str
) -> ApprovalAuditEntry | None:
    """The recovery audit row recorded under ``recovery_key``, on any approval.

    A key names one administrative act installation-wide, so the lookup is not
    scoped to an approval: the caller compares the row's ``approval_id`` to tell
    a replay from a key reused for a different approval.
    """

    result = await session.execute(
        select(_RecoveryAuditEntry)
        .where(
            _RecoveryAuditEntry.action == RECOVERY_AUDIT_ACTION,
            _RecoveryAuditEntry.evidence["recovery_key"].astext == recovery_key,
        )
        .order_by(_RecoveryAuditEntry.created_at)
        .limit(1)
    )
    return result.scalar_one_or_none()


async def recover_approval_atomic(
    session: AsyncSession,
    approval_id: uuid.UUID,
    *,
    reason: str,
    recovery_key: str,
    actor: str,
    actor_channel: str | None,
    principal_kind: str | None,
    facts: list[str],
) -> Approval | None:
    """Administratively settle a pending approval as ``rejected``, atomically.

    The CAS is guarded on ``status = 'pending'`` exactly as the ordinary
    resolve-once claim is, so an approval is settled at most once. Returns None
    when the CAS matched nothing; the caller looks the key up with
    ``find_recovery_audit`` to tell a replay (return the recorded outcome) from
    a genuine conflict.

    ``facts`` are the reporter's OBSERVATIONS, recorded as evidence. They state
    what was seen about the row. They never assert that the ordinary path was
    unavailable -- nothing here is in a position to know that.
    """

    recovered: uuid.UUID | None
    async with session.begin():
        # The associated publication, read inside the SAME transaction that
        # settles the approval. ``claim_approval_resolution`` settles it too,
        # but it commits internally, so it cannot be reused here: composing it
        # would put the publication's fate in a second transaction and reopen
        # the crash window this whole function exists to close.
        publication = await get_publication_by_approval(session, approval_id)
        values: dict[str, Any] = {
            "status": ApprovalStatus.rejected,
            "resolved_by": actor,
            "resolution_note": reason,
            "resolved_at": func.now(),
        }
        if publication is not None:
            # A publication outcome is reported by the platform worker through
            # the stored reply route, never by a resumed model turn. Mark the
            # wake as owing nothing in the same CAS, exactly as the ordinary
            # resolve path does, so the reconciler never picks the row up for a
            # resume the router deliberately does not enqueue.
            values["resumed_at"] = func.now()
        result = await session.execute(
            update(Approval)
            .where(
                Approval.id == approval_id,
                Approval.status == ApprovalStatus.pending,
            )
            .values(**values)
            .returning(Approval.id)
        )
        recovered = result.scalar_one_or_none()
        if recovered is None:
            return None
        if publication is not None:
            # The same denial the ordinary reject performs, under the same
            # version check: status denied, the patch dropped, the terminal
            # instant recorded. Without it the recovered approval is settled and
            # its publication waits forever -- the expiry sweeper no longer
            # selects a rejected approval, and no resume is enqueued to repair
            # it, so nothing else in the system would ever touch it again.
            changed = await session.execute(
                update(Publication)
                .where(
                    Publication.id == publication.id,
                    Publication.status == "pending",
                    Publication.version == publication.version,
                )
                .values(
                    status="denied",
                    version=Publication.version + 1,
                    updated_at=func.now(),
                    terminal_at=func.now(),
                    patch_bytes=None,
                )
                .returning(Publication.id)
            )
            if changed.scalar_one_or_none() is None:
                raise PublicationSettlementConflict(
                    "the approval's publication is no longer pending at the "
                    "version this recovery read; nothing was changed"
                )
        entry = ApprovalAuditEntry(
            approval_id=approval_id,
            action=RECOVERY_AUDIT_ACTION,
            actor=actor,
            actor_channel=actor_channel,
            principal_kind=principal_kind,
            authenticated=True,
            decision=ApprovalStatus.rejected,
            authorizer=_RECOVERY_AUTHORIZER,
            authorized=True,
            reason=reason,
            evidence={
                "kind": "administrative_recovery",
                "recovery_key": recovery_key,
                "facts": facts,
            },
        )
        session.add(entry)
    return await reread_approval(session, approval_id)


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


async def require_current_lineage_workspace(
    session: AsyncSession,
    lineage: ThreadPublicationLineage,
    *,
    conflict_code: str,
    conflict_message: str,
) -> None:
    """Recheck the workspace and deployment that still authorize publication."""

    workspace = await session.scalar(
        select(ThreadWorkspace)
        .where(
            ThreadWorkspace.agent_id == lineage.agent_id,
            ThreadWorkspace.conversation_id == lineage.conversation_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    deployment = await session.get(
        Deployment,
        lineage.deployment_id,
        with_for_update=True,
        populate_existing=True,
    )
    if (
        workspace is None
        or workspace.repo_full_name.casefold() != lineage.repo_full_name.casefold()
        or not repository_is_allowed(lineage.repo_full_name, get_settings().github_repo_allowlist)
        or deployment is None
        or deployment.agent_id != lineage.agent_id
        or deployment.status != "active"
    ):
        raise PublicationLineageConflict(
            conflict_code,
            conflict_message,
        )


async def _require_review_binding(
    session: AsyncSession, lineage: ThreadPublicationLineage
) -> AgentChannel:
    """Recheck original authority under the same transaction as reservation use."""

    binding = (
        await session.get(
            AgentChannel,
            lineage.binding_id,
            with_for_update=True,
            populate_existing=True,
        )
        if lineage.binding_id
        else None
    )
    await require_current_lineage_workspace(
        session,
        lineage,
        conflict_code="publication.review_ineligible",
        conflict_message="original review binding or workspace is no longer authorized",
    )
    if (
        binding is None
        or binding.agent_id != lineage.agent_id
        or binding.generation != lineage.binding_generation
        or not lineage.reply_conversation_id
        or scoped_conversation_id(binding.kind, binding.address, lineage.reply_conversation_id)
        != lineage.conversation_id
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
        or lineage.base_ref is None
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
        .execution_options(populate_existing=True)
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
            "publication.lineage_stale",
            "review expected a stale lineage version",
        )
    if (
        await publication_lineage_has_pending_revision(session, lineage)
        or await _publication_lineage_has_reserved_review(session, lineage)
        or await publication_lineage_has_pending_outcome(session, lineage)
    ):
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
            "publication.revision_conflict",
            "review origin is already reserved",
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
        .execution_options(populate_existing=True)
    )
    if row is None:
        raise LookupError("review reservation not found")
    if row.origin_key != origin_key or row.version != expected_version or row.status != "reserved":
        raise PublicationLineageConflict(
            "publication.revision_conflict",
            "review reservation changed before cancellation",
        )
    row.status = "cancelled"
    row.version += 1
    row.updated_at = func.now()
    await session.flush()
    return row
