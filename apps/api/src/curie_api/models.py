"""SQLAlchemy models: agents, agent_versions, deployments.

Kept deliberately minimal (see docs/build-orchestration-plan.md). B2 added the
bundle columns; J1 added the git-flow columns (agents.repo_full_name,
agent_versions.commit_sha, deployments.bot_identity/commit_sha).
"""

from __future__ import annotations

import enum
import secrets
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from .approval_wording import approval_display
from .db import SCHEMA, Base
from .repo_full_name import normalize_repo_full_name
from .sealed_snapshot import is_sealed_envelope

# Work-item execution deadline bounds (#3071). An agent's
# `execution_deadline_seconds` NULL means the default; a set value is bounded
# by the minimum and maximum, and the ExecutionRequest CHECK caps every row at
# the maximum.
DEFAULT_EXECUTION_DEADLINE_SECONDS = 1800
MIN_EXECUTION_DEADLINE_SECONDS = 60
MAX_EXECUTION_DEADLINE_SECONDS = 10800

GIT_FLOW_CREATED_BY = "git-flow"


class DeployNoticeOutbox(Base):
    """Credential-free, installation-scoped notice owed after git-flow settles."""

    __tablename__ = "deploy_notice_outbox"
    __table_args__ = (
        Index(
            "ix_deploy_notice_outbox_pending",
            "stream",
            "created_at",
            postgresql_where=text("enqueued_at IS NULL"),
        ),
        Index("ix_deploy_notice_outbox_enqueued_at", "enqueued_at"),
        Index("ix_deploy_notice_outbox_repo_window", "stream", "repo", "created_at"),
        {"schema": SCHEMA},
    )

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    stream: Mapped[str] = mapped_column(Text)
    # Case-folded repository full name, which the per-repository notice bound
    # counts by (docs/operations.md).
    repo: Mapped[str] = mapped_column(Text)
    payload: Mapped[str] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(server_default="0", default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    enqueued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class Environment(enum.StrEnum):
    prod = "prod"
    dev = "dev"


class ApprovalStatus(enum.StrEnum):
    """Lifecycle of a durable approval (ADR-0010). Stored as a plain string
    column (like ``Deployment.status``) so the resolve-once compare-and-set is
    a conditional UPDATE on the value, with these constants as the vocabulary."""

    pending = "pending"
    approved = "approved"
    rejected = "rejected"
    expired = "expired"


class ActionStatus(enum.StrEnum):
    """Lifecycle of one recorded action (ADR-0117).

    Two frames make one record. ``pending`` is the opening frame: the call was
    made and its result has not arrived. A turn that dies here leaves the row at
    ``pending`` forever, which is the honest state -- something may have changed
    and nothing came back to say what.
    """

    pending = "pending"
    succeeded = "succeeded"
    failed = "failed"


class Agent(Base):
    __tablename__ = "agents"
    __table_args__ = (
        CheckConstraint(
            "publication_policy IN ('approve', 'auto')",
            name="agents_publication_policy_ck",
        ),
        CheckConstraint(
            "publication_policy_version >= 1",
            name="agents_publication_policy_version_ck",
        ),
        CheckConstraint(
            "publication_branch_prefix IS NULL OR ("
            "char_length(publication_branch_prefix) BETWEEN 2 AND 64 "
            "AND publication_branch_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}/$' "
            "AND publication_branch_prefix NOT LIKE '%..%' "
            "AND publication_branch_prefix NOT LIKE '%.lock/' "
            "AND publication_branch_prefix NOT LIKE '%./')",
            name="agents_publication_branch_prefix_ck",
        ),
        CheckConstraint(
            "execution_deadline_seconds IS NULL OR execution_deadline_seconds "
            f"BETWEEN {MIN_EXECUTION_DEADLINE_SECONDS} AND {MAX_EXECUTION_DEADLINE_SECONDS}",
            name="agents_execution_deadline_seconds_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(unique=True)
    # The GitHub repo (owner/name) whose pushes deploy this agent (J1).
    #
    # NOT unique (ADR-0091, #1070). One repository legitimately builds several
    # agents -- a dev bot and a prod bot are the same bundle on two channels,
    # which is what a dev/prod split of a Slack bot is. Which agent a push
    # deploys to is answered by the bundle's `deploy.yaml`, not by the schema.
    # Indexed because git-flow looks agents up by this column on every webhook.
    #
    # Deliberately asymmetric with the `channel` binding below: two agents
    # sharing a repository is intended, two sharing a channel is silent
    # shadowing.
    repo_full_name: Mapped[str | None] = mapped_column(default=None, index=True)
    # Success notices are opt-in; a rejected push is always reported to bound
    # Slack channels because no deployment row may exist to inspect afterward.
    deploy_notifications: Mapped[bool] = mapped_column(default=False, server_default="false")

    @validates("repo_full_name")
    def _validate_repo_full_name(self, _key: str, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_repo_full_name(value)

    # Per-agent budget (L1). Field names match the frozen ACI SessionConfig
    # CURIE_BUDGET so the worker passes them straight through at sandbox boot;
    # NULL means platform defaults apply.
    max_usd_per_day: Mapped[float | None] = mapped_column(default=None)
    max_output_tokens_per_run: Mapped[int | None] = mapped_column(default=None)
    # Per-agent model id (#254). Forwarded as CURIE_MODEL at sandbox boot so a
    # single agent can be pinned to a specific model (BYO-model, #24); NULL means
    # the platform/worker default model applies. The value is passed straight
    # through to the runner, which resolves it against its configured provider.
    model: Mapped[str | None] = mapped_column(default=None)
    # Per-agent reviewer model (#4120). NULL lets the runner choose the
    # credential's default; a value becomes the SDK's Opus alias target.
    reviewer_model: Mapped[str | None] = mapped_column(default=None)
    # Per-agent thinking depth (#1182, ADR-0098). Forwarded as CURIE_THINKING at
    # sandbox boot; NULL means the worker's CURIE_THINKING default applies, and
    # unset at both layers means the runner sends no thinking configuration and
    # the model's own default stands. Operator-owned like `model` above: a bundle
    # has no surface for it at any tier. The vocabulary is the runner's
    # (`curie_runner.thinking`), not this column's -- stored as a plain string so
    # the persistence layer does not have to track the harness.
    thinking: Mapped[str | None] = mapped_column(default=None)
    # Per-agent work-item execution deadline in seconds (#3071). Operator-owned
    # like `model`/`thinking`; NULL means DEFAULT_EXECUTION_DEADLINE_SECONDS.
    execution_deadline_seconds: Mapped[int | None] = mapped_column(default=None)
    # Per-agent runner cpu, memory, and ephemeral-storage (#3209). NULL means
    # the chart agentSandbox.runner.resources block. A set value is applied on
    # the next sandbox claim, not by resizing a sandbox that is already running.
    runner_resources: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # Per-agent behavior packs: declarative, opt-in UX touches the worker applies
    # around a turn (a sampled "working..." line, a canned greeting reply). Stored
    # as JSON here and resolved onto the deployment by the worker's binding layer;
    # NULL means no packs (the platform default). The shape is validated by
    # schemas.agents.BehaviorPacksConfig on write and parsed by
    # curie_worker.behaviorpacks.BehaviorPacks on read.
    behavior_packs: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # Per-agent permission gates (#245, ADR-0010): tool names whose calls
    # require human approval. Forwarded by the worker binding as
    # CURIE_APPROVAL_REQUIRED_TOOLS at sandbox boot; the runner's
    # can_use_tool callback blocks these calls and ends the turn
    # awaiting-approval. NULL means no permission gates (the bypass posture).
    approval_required_tools: Mapped[list[str] | None] = mapped_column(JSONB, default=None)
    # Per-agent approval route bindings (#247, ADR-0010): the workspace half of
    # the split policy. The bundle manifest declares gate points and route
    # NAMES (versioned with the agent); this maps each declared name to
    # workspace specifics: one Slack-only resolution target and an optional
    # channel-neutral notification target. The worker posts the sole resolving
    # card at ``resolution`` (whose address is persisted on the Approval row for
    # authorization) and may send a text-only ping at ``notification``. NULL
    # means no bindings; a named unbound route escalates rather than widening to
    # the requesting channel.
    approval_routes: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # Per-agent connector secrets (ADR-0009, #429): the named secret VALUES the
    # bundle's authed MCP servers need (e.g. GITHUB_PERSONAL_ACCESS_TOKEN). The
    # bundle declares the NAMES (plugin-format `secrets`); values are supplied at
    # deploy and stored here for the LOCAL tier. The worker binding injects them
    # by name into the sandbox boot env, where `.mcp.json` `${VAR}` expansion
    # consumes them. NULL means no connector secrets. (The cluster tier delivers
    # values via a per-agent K8s Secret instead; only the names live here there.)
    secrets: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # Rotation counter for this agent's inbound hook secret (ADR-0079, #269).
    #
    # NOT a secret, which is the point. The secret an upstream signs with is
    # DERIVED from the platform key, this agent's id and this number
    # (`hook_secret.derive`), so nothing a reader of this table finds lets them
    # forge a delivery. Bumping the counter rotates exactly one agent's hook
    # secret; storing the secret itself would have meant a third-party credential
    # sitting in plaintext in the control plane, and rotating it any other way
    # means rotating the platform key for every agent at once.
    hook_generation: Mapped[int] = mapped_column(default=0, server_default="0")
    # Which of this agent's hooks fan out, and by what (ADR-0134, amending
    # ADR-0079): hook name -> ``{"pointer": <RFC 6901 pointer>}``, the pointer
    # naming the field of a delivery body that identifies the thing the delivery
    # is about. NULL means no hook on this agent partitions, which is the
    # behavior every hook had before the column existed: one thread per hook.
    #
    # The pointer must name a STABLE identity of that thing -- a pull request
    # number, a ticket key, a thread ts -- and never a run id or a timestamp. A
    # partition IS a thread and owns a transcript, so an identity that changes
    # per delivery makes every transcript single-use and grows the state store
    # without bound. Nothing secret belongs here either (the neighbouring
    # `hook_generation` comment's discipline): a pointer is configuration, and it
    # must not be extended into anything carrying a VALUE from the payload.
    hook_partitions: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # Operator-controlled workload to repository map for inbound hooks (#2572).
    # Hook name -> ``{"workload_pointer": <RFC 6901>, "map": {workload: {repository,
    # revision}}}``. NULL means no hook on this agent selects a coding target
    # from a delivery: investigation may still run, coding does not guess a repo.
    source_bindings: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # Who resolves a publication approval (ADR 0147). ``approve`` is today's
    # human gate and the value every pre-existing row receives. ``auto`` makes
    # the platform resolve the same approval row under the recorded policy.
    # The version increments when the operator changes the policy or its bounds,
    # so an in-flight auto approval cannot redeem after that change.
    publication_policy: Mapped[str] = mapped_column(
        default="approve", server_default="approve"
    )
    publication_policy_version: Mapped[int] = mapped_column(default=1, server_default="1")
    publication_draft: Mapped[bool] = mapped_column(default=False, server_default="false")
    publication_branch_prefix: Mapped[str | None] = mapped_column(default=None)
    # Whether this agent's bindings share one workflow-state namespace or each
    # get their own (#1525 follow-up). Cardinality alone (ADR-0118 decision 2)
    # governs routing and agent-scoped controls (budget, kill state, bundle
    # version) unconditionally -- those are never gated by this column. This
    # ONLY decides `workflow_state_entries.binding_scope`: False (default) keys
    # every binding's state store separately, so an agent invited to a second
    # channel it never explicitly opted into sharing does not silently start
    # mixing that channel's state into the first's. True shares one namespace
    # across every binding. Existing single-binding agents are unaffected
    # either way, since there is nothing else to share with.
    memory: Mapped[bool] = mapped_column(default=False, server_default="false")
    # Whether the runner mounts its remember/update/forget memory tools for this
    # agent (#1461, ADR-0167). Operator-owned; off by default. When on, the
    # worker hands the runner the binding-scoped channel memory URL.
    memory_writes: Mapped[bool] = mapped_column(default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    versions: Mapped[list[AgentVersion]] = relationship(
        back_populates="agent", cascade="all, delete-orphan"
    )
    # The agent's channel bindings (ADR-0096, #1459; PLURAL since ADR-0118,
    # migration 0030): an agent may hold more than one, so the API surface is a
    # list rather than an object.
    #
    # `order_by` is load-bearing, not cosmetic: `agent_channels` has no
    # `created_at` to fall back on, so without an explicit order the serialized
    # list's element order is whatever Postgres happens to return, and two
    # identical GETs could differ. `(kind, address, adapter)` is used because it
    # is the route every other layer keys by (`agent_channels_route_key`).
    #
    # `lazy="selectin"` is load-bearing, not a preference: every read path builds
    # `AgentOut` from this attribute after its session has been handed back, and
    # the default lazy strategy RAISES on attribute access outside an await under
    # asyncio instead of loading. Dropping it turns all three read endpoints into
    # 500s while the crud-level tests, which hold a live session, stay green.
    channels: Mapped[list[AgentChannel]] = relationship(
        back_populates="agent",
        cascade="all, delete-orphan",
        order_by="(AgentChannel.kind, AgentChannel.address, AgentChannel.adapter)",
        lazy="selectin",
    )


class AgentChannel(Base):
    """Where one agent listens: a channel KIND and an ADDRESS (ADR-0096, #1459).

    Replaces `agents.slack_channel` (migration 0021) so an agent can bind a
    channel kind the platform has never heard of without a schema change.

    `kind` names the owning adapter, selects the address-shape validator
    (`schemas.channels.validate_channel_binding`), AND routes: since ADR-0096 phase 2 the
    queue wire carries a required `ReplyHandle.kind`, so the worker resolves on
    the PAIR and the uniqueness below widened to match (migration 0023). The
    widening was only safe once no address-only consumer could run -- a
    pair-unique constraint under an address-only lookup would let two agents hold
    one address while the resolver could not tell them apart, which is #38's
    silent misrouting wearing a different hat. That ordering is why 0023 lands
    after the cutover proves no old worker is running. Migration 0070 widens the
    key to `(kind, address, adapter)` (ADR-0168 decision 3).

    `endpoint`/`adapter` are the server-controlled reply route: where this kind's
    replies go back through, and which egress credential authenticates them. They
    are set here by the platform and never accepted from an ingress request body.
    `generation` counts rotations: `update_channel_binding` mutates this row IN
    PLACE, and `POST /channels/token` bumps it on every mint, so the row id is a
    stable identity and the generation is the only thing that makes a rebind or
    remint observable to a credential minted before it.

    `allowed_callers` (ADR 0175, migration 0068) is who may start a turn through
    this binding: NULL for everyone, else the exact caller ids `admission.admit`
    matches. It is written only by its own endpoint, which leaves `generation`
    alone, because who may use a route is a separate question from the route.
    """

    __tablename__ = "agent_channels"
    __table_args__ = (
        # One agent per ROUTE, the `(kind, address, adapter)` triple (ADR-0168
        # decision 3, migration 0070; 0023 keyed the pair, 0021 the address).
        # A second agent bound to the same route could never respond -- it
        # would be silently shadowed (#38). Enforced here so it fails at create
        # time. The pair leads so `(kind, address)` lookups keep the index
        # prefix, and NULLS NOT DISTINCT keeps two route-less non-Slack rows
        # colliding on the pair.
        UniqueConstraint(
            "kind",
            "address",
            "adapter",
            name="agent_channels_route_key",
            postgresql_nulls_not_distinct=True,
        ),
        CheckConstraint(
            "(kind = 'slack' AND adapter IS NOT NULL AND endpoint IS NULL) "
            "OR (kind <> 'slack' AND (endpoint IS NULL) = (adapter IS NULL))",
            name="agent_channels_route_ck",
        ),
        # No agent_id uniqueness here (ADR-0118, migration 0030): an agent may
        # hold more than one binding now. ADR-0089's "one agent still binds one
        # channel" is amended in part -- the route key above is what stops two
        # agents claiming the same route; nothing stops one agent from claiming
        # several.
        #
        # PLAIN index on agent_id, because dropping that uniqueness dropped the
        # column's only index with it (migration 0030 recreates it as this).
        # `crud.channels.lock_agent_bindings` filters and orders by agent_id under
        # `FOR UPDATE` on every add, move and delete; unindexed, each of those
        # scans the whole table while holding locks.
        Index("ix_agent_channels_agent_id", "agent_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE")
    )
    kind: Mapped[str]
    address: Mapped[str]
    # The reply route (migration 0024) and, for `slack`, the bot identity
    # (ADR-0168 decision 3): a Slack row names its identity in `adapter` and has
    # no `endpoint`; any other kind sets both or neither.
    # `agent_channels_route_ck` states it at the database so a half-configured
    # route cannot be written out of band.
    endpoint: Mapped[str | None] = mapped_column(default=None)
    adapter: Mapped[str | None] = mapped_column(default=None)
    # Rotation counter (ADR-0096 D5, #2379). Bumped on every write to the ROUTE
    # (a move or re-assert through `update_channel_binding`, including one that
    # changes nothing) and on every `POST /channels/token` mint: re-asserting a
    # binding or reminting its credential both invalidate outstanding tokens.
    # Editing `allowed_callers` below does NOT bump it (ADR 0175 decision 4).
    generation: Mapped[int] = mapped_column(server_default="0", default=0)
    # Who may start a turn through this binding (ADR 0175, migration 0068).
    # NULL means everyone; a list is never empty (the API refuses it and
    # `agent_channels_allowed_callers_ck` states it at the database).
    # `none_as_null` is load-bearing: without it a Python None is stored as the
    # JSON value `null`, which is not SQL NULL and fails that CHECK.
    allowed_callers: Mapped[list[str] | None] = mapped_column(
        JSONB(none_as_null=True), default=None
    )

    agent: Mapped[Agent] = relationship(back_populates="channels")


class AgentVersion(Base):
    __tablename__ = "agent_versions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), index=True
    )
    version_label: Mapped[str]
    bundle_ref: Mapped[str | None] = mapped_column(default=None)
    bundle_sha256: Mapped[str | None] = mapped_column(default=None)
    # The git commit this version was built from (J1); lets promote reuse the
    # already-built bundle instead of rebuilding.
    commit_sha: Mapped[str | None] = mapped_column(default=None, index=True)
    created_by: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    agent: Mapped[Agent] = relationship(back_populates="versions")


class Deployment(Base):
    __tablename__ = "deployments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), index=True
    )
    version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agent_versions.id", ondelete="CASCADE"), index=True
    )
    environment: Mapped[Environment] = mapped_column(
        Enum(Environment, name="environment", schema=SCHEMA)
    )
    commit_sha: Mapped[str | None] = mapped_column(default=None)
    # A deployment-level capability. The concrete repository is selected from
    # the opening thread message and stored in ThreadWorkspace only after the
    # operator allowlist authorizes it.
    workspace_enabled: Mapped[bool] = mapped_column(default=False)
    status: Mapped[str] = mapped_column(server_default="active")
    deployed_at: Mapped[datetime] = mapped_column(server_default=func.now())


class ThreadWorkspace(Base):
    """One immutable repository selection for an agent conversation."""

    __tablename__ = "thread_workspaces"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), index=True
    )
    selected_by_deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.deployments.id", ondelete="SET NULL"), default=None
    )
    conversation_id: Mapped[str]
    repo_full_name: Mapped[str]
    revision: Mapped[str | None] = mapped_column(default=None)
    selected_by: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "agent_id", "conversation_id", name="thread_workspaces_agent_conversation_key"
        ),
    )


class Approval(Base):
    """A durable approval request (#244, ADR-0010).

    Created by the worker when a run ends ``awaiting-approval``; the session is
    suspended while this row is pending, so the record must carry everything a
    later resume needs (the conversation key and the reply handle) -- the pause
    survives full component restarts because nothing lives in memory.

    Resolve-once claim semantics: resolution is a conditional UPDATE guarded on
    ``status = 'pending'`` (compare-and-set), so exactly one resolver wins and
    losers are told who resolved it. ``dedupe_key`` (the triggering event id)
    makes record creation idempotent under the worker's at-least-once redelivery.
    """

    __tablename__ = "approvals"
    __table_args__ = (
        CheckConstraint(
            "(policy_identity IS NULL AND policy_version IS NULL) OR "
            "(policy_identity = 'publication:auto' AND policy_version >= 1)",
            name="approvals_policy_identity_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Nullable: a run without a deployment binding (the generic/dev path) can
    # still gate on a human decision.
    agent_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        index=True,
        default=None,
    )
    # Adapter-native, bare conversation identity. The resume path combines it
    # with the stored reply kind/channel when it needs the worker's scoped
    # thread key; adapter egress continues to receive this unmodified value.
    conversation_id: Mapped[str] = mapped_column(index=True)
    # Who authored the turn that raised the request. ADR-0106 permits that same
    # authenticated principal to resolve only when the selected set admits it.
    author: Mapped[str]
    # The human-readable statement of what needs approval, from the run's
    # approval request (the ACI final's approval_summary).
    summary: Mapped[str]
    @property
    def display_summary(self) -> str:
        """Computed presentation; grants still bind to the stored exact fields."""
        return approval_display(self.summary, self.granted_tool, self.granted_arguments)

    # The reply handle of the requesting turn, replayed onto the resume turn so
    # the resumed run streams into the same placeholder message.
    #
    # `reply_kind` is the durable twin of `ReplyHandle.kind` (ADR-0096 phase 2):
    # NOT NULL with no default, because a resume rebuilt from a fabricated kind
    # is the silent misroute at its least observable point. Its safety on
    # pre-existing rows comes from migration 0022's provenance preflight and the
    # quiescent cutover, not from a claim that every old approval was Slack.
    # `reply_adapter` is the durable twin of `ReplyHandle.adapter`, nullable
    # because `slack` legitimately has none.
    reply_kind: Mapped[str]
    reply_channel: Mapped[str]
    reply_placeholder: Mapped[str | None] = mapped_column(nullable=True)
    reply_endpoint: Mapped[str | None] = mapped_column(default=None)
    reply_adapter: Mapped[str | None] = mapped_column(default=None)
    # The approval route the request named (#247), and the channel the card
    # was actually routed to after binding resolution. The authorizer proves
    # channel membership against card_channel (falling back to reply_channel
    # when NULL, the pre-route behavior).
    route: Mapped[str | None] = mapped_column(default=None)
    card_channel: Mapped[str | None] = mapped_column(default=None)
    # Idempotency: the triggering event id. A reclaimed/redelivered turn that
    # re-requests the same approval adopts the existing row instead of forking.
    dedupe_key: Mapped[str] = mapped_column(unique=True)
    # Private W3C carrier for the worker turn that created this record. It is
    # deliberately absent from every request/response DTO: the authenticated
    # ingress route derives it from the HTTP header, and terminal/recovery paths
    # use it only to reconnect telemetry after the human pause.
    traceparent: Mapped[str | None] = mapped_column(String(55), default=None)
    status: Mapped[str] = mapped_column(server_default=ApprovalStatus.pending, index=True)
    # Optional SLA: past this instant the record can no longer be approved or
    # rejected; a resolve attempt flips it to expired instead.
    expires_at: Mapped[datetime | None] = mapped_column(default=None)
    resolved_by: Mapped[str | None] = mapped_column(default=None)
    resolution_note: Mapped[str | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    resolved_at: Mapped[datetime | None] = mapped_column(default=None)
    # Set once the resume turn is enqueued onto the runs stream (#411); NULL on a
    # resolved record means the wake is still owed (the reconciler's work-list).
    resumed_at: Mapped[datetime | None] = mapped_column(default=None)
    # Durable gate provenance (#544, Decision C), written by the runner -- the
    # only component that knows which tool ``can_use_tool`` denied. ``gate_kind``
    # is ``'permission'`` when the tool-permission gate denied a real tool call,
    # ``'policy'`` when the model asked for a business-decision approval; it is
    # the column the worker branches on instead of sniffing the summary prefix.
    # ``granted_tool`` is the tool name a resume-turn grant is bound to. It is set
    # for ``gate_kind='permission'`` and, since #558, for a ``gate_kind='policy'``
    # gate the operator opted into grantability (grantableViaPolicy) -- the runner
    # stamps the manifest tool onto it for those gates and leaves it NULL for
    # every other policy gate. Both NULL from an older runner that predates them,
    # which is the rolling-deploy window the worker's prefix fallback covers.
    gate_kind: Mapped[str | None] = mapped_column(default=None)
    granted_tool: Mapped[str | None] = mapped_column(default=None)
    # Canonical arguments of the denied permission gated call. NULL on old
    # approvals and policy gates; an empty object is a real argument value.
    # Kept private to the worker's resume lookup bound to the agent.
    granted_arguments: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True), default=None
    )
    # Server-owned purpose. ``publication`` suppresses the ordinary model wake;
    # requester equality follows the same approver-set rule for every purpose.
    purpose: Mapped[str] = mapped_column(server_default="session", default="session")
    # Set only when the platform resolved this row under ADR 0147. Human
    # resolutions leave both NULL so they stay distinguishable in the audit.
    policy_identity: Mapped[str | None] = mapped_column(default=None)
    policy_version: Mapped[int | None] = mapped_column(default=None)

    publication: Mapped[Publication | None] = relationship(back_populates="approval", uselist=False)


class ThreadPublicationLineage(Base):
    """One durable pull-request identity owned by an agent conversation."""

    __tablename__ = "thread_publication_lineages"
    __table_args__ = (
        CheckConstraint(
            "status IN ('open', 'merged', 'closed')",
            name="thread_publication_lineages_status_ck",
        ),
        CheckConstraint(
            "version >= 1",
            name="thread_publication_lineages_version_ck",
        ),
        CheckConstraint(
            "latest_revision >= 1",
            name="thread_publication_lineages_latest_revision_ck",
        ),
        CheckConstraint(
            "(pr_number IS NULL) = (pr_url IS NULL)",
            name="thread_publication_lineages_pr_identity_ck",
        ),
        Index(
            "uq_active_thread_publication_lineage",
            "agent_id",
            "conversation_id",
            "repo_full_name",
            unique=True,
            postgresql_where=text("status = 'open'"),
        ),
        CheckConstraint(
            "(github_repository_id IS NULL AND github_installation_id IS NULL "
            "AND github_pr_node_id IS NULL AND base_ref IS NULL) "
            "OR (github_repository_id IS NOT NULL "
            "AND github_repository_id > 0 "
            "AND github_installation_id IS NOT NULL AND github_installation_id > 0 "
            "AND github_pr_node_id IS NOT NULL "
            "AND length(github_pr_node_id) > 0 AND pr_number IS NOT NULL "
            "AND base_ref IS NOT NULL AND length(base_ref) > 0)",
            name="thread_publication_lineages_github_identity_ck",
        ),
        Index(
            "uq_publication_github_pr_owner",
            "github_repository_id",
            "pr_number",
            unique=True,
        ),
        Index(
            "uq_active_publication_github_conversation",
            "agent_id",
            "conversation_id",
            "github_repository_id",
            unique=True,
            postgresql_where=text("status = 'open'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), index=True
    )
    deployment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.deployments.id", ondelete="CASCADE"), index=True
    )
    conversation_id: Mapped[str] = mapped_column(index=True)
    repo_full_name: Mapped[str]
    base_sha: Mapped[str]
    branch: Mapped[str] = mapped_column(unique=True)
    pr_number: Mapped[int | None] = mapped_column(default=None)
    pr_url: Mapped[str | None] = mapped_column(default=None)
    head_sha: Mapped[str | None] = mapped_column(default=None)
    status: Mapped[str] = mapped_column(server_default="open", default="open")
    version: Mapped[int] = mapped_column(server_default="1", default=1)
    latest_revision: Mapped[int] = mapped_column(server_default="1", default=1)
    # Only new trusted publication creation captures routing authority. Historical
    # NULL rows are deliberately never backfilled from a currently reused name.
    binding_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.agent_channels.id", ondelete="SET NULL"),
        default=None,
    )
    binding_generation: Mapped[int | None] = mapped_column(default=None)
    reply_conversation_id: Mapped[str | None] = mapped_column(default=None)
    github_repository_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    github_installation_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    github_pr_node_id: Mapped[str | None] = mapped_column(default=None)
    base_ref: Mapped[str | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    publications: Mapped[list[Publication]] = relationship(back_populates="lineage")


class WorkItem(Base):
    """Durable execution identity for one canonical GitHub issue."""

    __tablename__ = "work_items"
    __table_args__ = (
        CheckConstraint(
            "github_repository_id > 0",
            name="work_items_github_repository_id_ck",
        ),
        CheckConstraint(
            "github_issue_number > 0",
            name="work_items_github_issue_number_ck",
        ),
        CheckConstraint(
            "github_installation_id > 0",
            name="work_items_github_installation_id_ck",
        ),
        CheckConstraint("version >= 1", name="work_items_version_ck"),
        CheckConstraint("next_sequence >= 1", name="work_items_next_sequence_ck"),
        UniqueConstraint(
            "github_repository_id",
            "github_issue_number",
            name="work_items_github_issue_key",
        ),
        UniqueConstraint(
            "publication_lineage_id",
            name="work_items_publication_lineage_key",
        ),
        CheckConstraint(
            "(readmit_request_id IS NULL AND readmit_requester IS NULL "
            "AND readmit_objective IS NULL) OR "
            "(readmit_request_id IS NOT NULL AND readmit_requester IS NOT NULL "
            "AND readmit_objective IS NOT NULL)",
            name="work_items_readmit_ck",
        ),
        CheckConstraint(
            "(base_branch IS NULL AND base_source IS NULL AND base_commit IS NULL) OR "
            "(base_branch IS NOT NULL AND base_source IS NOT NULL "
            "AND base_commit IS NOT NULL)",
            name="work_items_base_ck",
        ),
        CheckConstraint(
            "base_source IS NULL OR base_source IN ('label', 'default')",
            name="work_items_base_source_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    github_repository_id: Mapped[int] = mapped_column(BigInteger)
    github_issue_number: Mapped[int]
    github_installation_id: Mapped[int] = mapped_column(BigInteger)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE")
    )
    repo_full_name: Mapped[str]
    conversation_id: Mapped[str]
    publication_lineage_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            f"{SCHEMA}.thread_publication_lineages.id",
            ondelete="RESTRICT",
        ),
        default=None,
    )
    cancelled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    # A relabel that arrived while a request was still running. The
    # reconciler admits it once that request reaches a terminus.
    readmit_request_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), default=None
    )
    readmit_requester: Mapped[str | None] = mapped_column(Text, default=None)
    readmit_objective: Mapped[str | None] = mapped_column(Text, default=None)
    # The base resolved for that deferred relabel (ADR 0186). It replaces the
    # recorded base only when the replacement request is admitted.
    readmit_base_branch: Mapped[str | None] = mapped_column(Text, default=None)
    readmit_base_source: Mapped[str | None] = mapped_column(Text, default=None)
    readmit_base_commit: Mapped[str | None] = mapped_column(Text, default=None)
    # The base resolved at admission and frozen for every later run (ADR
    # 0186). All three are NULL on a legacy row, which uses the repository
    # default branch. ``base_label_ignored`` is the branch a later ``base:``
    # label names when it disagrees with the recorded one.
    base_branch: Mapped[str | None] = mapped_column(Text, default=None)
    base_source: Mapped[str | None] = mapped_column(Text, default=None)
    base_commit: Mapped[str | None] = mapped_column(Text, default=None)
    base_label_ignored: Mapped[str | None] = mapped_column(Text, default=None)
    version: Mapped[int] = mapped_column(default=1, server_default="1")
    next_sequence: Mapped[int] = mapped_column(default=1, server_default="1")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    publication_lineage: Mapped[ThreadPublicationLineage | None] = relationship()
    execution_requests: Mapped[list[ExecutionRequest]] = relationship(
        back_populates="work_item",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ExecutionRequest(Base):
    """One bounded execution attempt owned by a WorkItem."""

    __tablename__ = "execution_requests"
    __table_args__ = (
        CheckConstraint("sequence > 0", name="execution_requests_sequence_ck"),
        CheckConstraint("version >= 1", name="execution_requests_version_ck"),
        CheckConstraint(
            "status IN ('queued', 'cancelled') OR wait_deadline IS NOT NULL",
            name="execution_requests_wait_deadline_ck",
        ),
        CheckConstraint(
            "status IS NOT NULL AND status IN "
            "('queued', 'waiting', 'running', 'cancellation_requested', 'completed', "
            "'failed', 'expired', 'cancelled')",
            name="execution_requests_status_ck",
        ),
        CheckConstraint(
            "terminal_cause IS NULL OR length(btrim(terminal_cause)) > 0",
            name="execution_requests_terminal_cause_ck",
        ),
        CheckConstraint(
            "termination_observation IS NULL "
            "OR length(btrim(termination_observation)) > 0",
            name="execution_requests_termination_observation_ck",
        ),
        CheckConstraint(
            "(started_at IS NULL AND execution_deadline IS NULL) OR "
            "(started_at IS NOT NULL AND execution_deadline IS NOT NULL AND "
            "execution_deadline > started_at AND "
            "execution_deadline <= started_at + "
            f"interval '{MAX_EXECUTION_DEADLINE_SECONDS} seconds')",
            name="execution_requests_deadline_ck",
        ),
        CheckConstraint(
            "((status = 'queued' AND wait_deadline IS NULL AND started_at IS NULL "
            "AND execution_deadline IS NULL AND terminal_at IS NULL "
            "AND terminal_cause IS NULL AND termination_observation IS NULL) "
            "OR (status = 'waiting' AND wait_deadline IS NOT NULL AND started_at IS NULL "
            "AND execution_deadline IS NULL AND terminal_at IS NULL "
            "AND terminal_cause IS NULL AND termination_observation IS NULL) "
            "OR (status = 'running' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
            "AND terminal_cause IS NULL AND termination_observation IS NULL) "
            "OR (status = 'cancellation_requested' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause IN ('issue_cancelled', 'execution_deadline', 'owner_lost') "
            "AND termination_observation IS NULL) "
            "OR (status = 'completed' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
            "AND terminal_cause IS NOT NULL AND terminal_cause = 'completed' "
            "AND termination_observation IS NULL) "
            "OR (status = 'failed' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
            "AND terminal_cause IS NOT NULL AND "
            "((terminal_cause = 'owner_lost' AND termination_observation IS NOT NULL) "
            "OR (terminal_cause <> 'owner_lost' AND termination_observation IS NULL))) "
            "OR (status = 'expired' AND terminal_at IS NOT NULL AND "
            "((started_at IS NULL AND execution_deadline IS NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause = 'capacity_wait_expired' "
            "AND termination_observation IS NULL) OR "
            "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause = 'execution_deadline' "
            "AND termination_observation IS NOT NULL))) "
            "OR (status = 'cancelled' AND terminal_at IS NOT NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause IN ('issue_cancelled', 'lineage_closed') AND "
            "((started_at IS NULL AND execution_deadline IS NULL "
            "AND termination_observation IS NULL) OR "
            "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
            "AND termination_observation IS NOT NULL)))) IS TRUE",
            name="execution_requests_state_shape_ck",
        ),
        CheckConstraint(
            "teardown_unconfirmed_at IS NULL OR status = 'cancelled'",
            name="execution_requests_teardown_unconfirmed_ck",
        ),
        CheckConstraint(
            "dispatch_generation >= 1",
            name="execution_requests_dispatch_generation_ck",
        ),
        CheckConstraint(
            "published_generation IS NULL OR "
            "published_generation BETWEEN 1 AND dispatch_generation",
            name="execution_requests_published_generation_ck",
        ),
        CheckConstraint(
            "acquired_generation IS NULL OR "
            "acquired_generation BETWEEN 1 AND dispatch_generation",
            name="execution_requests_acquired_generation_ck",
        ),
        CheckConstraint(
            "capacity_deferrals >= 0",
            name="execution_requests_capacity_deferrals_ck",
        ),
        CheckConstraint(
            "dispatch_epoch >= 0",
            name="execution_requests_dispatch_epoch_ck",
        ),
        CheckConstraint(
            "runtime_epoch >= 0",
            name="execution_requests_runtime_epoch_ck",
        ),
        CheckConstraint(
            "execution_attempts IN (0, 1) AND "
            "(execution_attempts = 1) = (started_at IS NOT NULL)",
            name="execution_requests_execution_attempts_ck",
        ),
        CheckConstraint(
            "(objective IS NULL AND requester IS NULL AND reply_kind IS NULL "
            "AND reply_address IS NULL AND reply_conversation_id IS NULL) OR "
            "(objective IS NOT NULL AND requester IS NOT NULL AND "
            "reply_kind IS NOT NULL AND reply_address IS NOT NULL AND "
            "reply_conversation_id IS NOT NULL AND length(btrim(objective)) > 0 "
            "AND length(objective) <= 65536 AND length(btrim(requester)) > 0 "
            "AND length(btrim(reply_kind)) > 0 AND length(btrim(reply_address)) > 0 "
            "AND length(btrim(reply_conversation_id)) > 0)",
            name="execution_requests_snapshot_ck",
        ),
        UniqueConstraint(
            "work_item_id",
            "sequence",
            name="execution_requests_work_item_sequence_key",
        ),
        Index(
            "uq_execution_requests_active_work_item",
            "work_item_id",
            unique=True,
            postgresql_where=text(
                "status IN ('waiting', 'running', 'cancellation_requested')"
            ),
        ),
        Index(
            "ix_execution_requests_dispatch_due",
            "dispatch_not_before",
            postgresql_where=text("status = 'waiting'"),
        ),
        Index(
            "ix_execution_requests_queued",
            "work_item_id",
            "sequence",
            postgresql_where=text("status = 'queued'"),
        ),
        Index(
            "ix_execution_requests_runtime_liveness",
            "runtime_heartbeat_expires_at",
            postgresql_where=text(
                "status IN ('running','cancellation_requested')"
            ),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    work_item_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.work_items.id", ondelete="CASCADE")
    )
    sequence: Mapped[int]
    status: Mapped[str] = mapped_column(default="waiting", server_default="waiting")
    wait_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    execution_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    terminal_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    terminal_cause: Mapped[str | None] = mapped_column(default=None)
    termination_observation: Mapped[str | None] = mapped_column(
        Text, default=None
    )
    version: Mapped[int] = mapped_column(default=1, server_default="1")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    dispatch_generation: Mapped[int] = mapped_column(default=1, server_default="1")
    published_generation: Mapped[int | None] = mapped_column(default=None)
    dispatch_not_before: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    dispatch_owner: Mapped[str | None] = mapped_column(Text, default=None)
    dispatch_epoch: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default="0"
    )
    dispatch_lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    acquired_generation: Mapped[int | None] = mapped_column(default=None)
    acquire_owner: Mapped[str | None] = mapped_column(Text, default=None)
    acquire_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    capacity_deferrals: Mapped[int] = mapped_column(default=0, server_default="0")
    last_deferral_reason: Mapped[str | None] = mapped_column(Text, default=None)
    execution_attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    runtime_owner: Mapped[str | None] = mapped_column(Text, default=None)
    runtime_epoch: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default="0"
    )
    runtime_heartbeat_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    terminate_published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    # Settle anchor: updated_at moves on heartbeats and terminate publishes.
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    # A forced settle with no worker teardown receipt. Terminate wakes keep
    # going out until a worker records the teardown and clears this.
    teardown_unconfirmed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    runtime_claim_name: Mapped[str | None] = mapped_column(Text, default=None)
    runtime_sandbox_name: Mapped[str | None] = mapped_column(Text, default=None)
    objective: Mapped[str | None] = mapped_column(Text, default=None)
    requester: Mapped[str | None] = mapped_column(Text, default=None)
    reply_kind: Mapped[str | None] = mapped_column(Text, default=None)
    reply_address: Mapped[str | None] = mapped_column(Text, default=None)
    reply_conversation_id: Mapped[str | None] = mapped_column(Text, default=None)

    work_item: Mapped[WorkItem] = relationship(back_populates="execution_requests")


def new_card_token() -> str:
    """A fresh unguessable card capability: 32 random bytes as lowercase hex."""

    return secrets.token_hex(32)


class FactoryStatusComment(Base):
    """One bot-authored GitHub status comment per factory execution request.

    Inserted at admission and edited in place by the reconciler as the request
    progresses (#3077). The terminus fills ``terminal_cause`` and ``detail``. A
    refusal is recorded here and does not rewrite the request. ``posted_at``
    means the comment was created; ``finalized_at`` means it will not be edited
    again.
    """

    # Keeps its pre-#3077 table name so 0055 stays an expand revision.
    __tablename__ = "factory_terminal_notices"
    __table_args__ = (
        CheckConstraint(
            "terminal_cause IS NULL OR length(btrim(terminal_cause)) > 0",
            name="factory_terminal_notices_cause_ck",
        ),
        CheckConstraint("attempts >= 0", name="factory_terminal_notices_attempts_ck"),
        CheckConstraint("scan_page >= 1", name="factory_terminal_notices_scan_page_ck"),
        CheckConstraint(
            "posted_at IS NULL OR refused_at IS NULL",
            name="factory_terminal_notices_one_outcome_ck",
        ),
        CheckConstraint(
            "(comment_id IS NULL) = (posted_at IS NULL)",
            name="factory_terminal_notices_comment_ck",
        ),
        CheckConstraint(
            "(refusal IS NULL) = (refused_at IS NULL)",
            name="factory_terminal_notices_refusal_ck",
        ),
        CheckConstraint(
            "comment_list IS NULL OR comment_list IN ('issue', 'review')",
            name="factory_terminal_notices_comment_list_ck",
        ),
        CheckConstraint(
            "comment_list IS NULL OR comment_id IS NOT NULL",
            name="factory_terminal_notices_comment_list_pair_ck",
        ),
        CheckConstraint(
            "finalized_at IS NULL OR posted_at IS NOT NULL",
            name="factory_terminal_notices_finalized_ck",
        ),
        CheckConstraint(
            "subject_title IS NULL OR length(subject_title) <= 256",
            name="factory_terminal_notices_subject_title_ck",
        ),
        # The four curie:* values are legacy, accepted so application N-1 can
        # still write them, and are not the labels the reconciler applies.
        CheckConstraint(
            "applied_label IS NULL OR applied_label IN "
            "('', 'curie:queued', 'curie:running', 'curie:pr-open', 'curie:needs-human', "
            "'curie-factory:queued', 'curie-factory:running', 'curie-factory:pr-open', "
            "'curie-factory:needs-human')",
            name="factory_terminal_notices_applied_label_ck",
        ),
        # Application N-1's delivery scan still reads this one.
        Index(
            "ix_factory_terminal_notices_pending",
            "created_at",
            postgresql_where=text("posted_at IS NULL AND refused_at IS NULL"),
        ),
        Index(
            "ix_factory_terminal_notices_unfinalized",
            "created_at",
            postgresql_where=text("finalized_at IS NULL AND refused_at IS NULL"),
        ),
    )

    execution_request_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.execution_requests.id", ondelete="CASCADE"),
        primary_key=True,
    )
    work_item_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.work_items.id", ondelete="CASCADE")
    )
    # The capability in the card URL camo fetches: 64 lowercase hex characters.
    card_token: Mapped[str] = mapped_column(
        Text,
        unique=True,
        default=new_card_token,
        server_default=text(
            "replace(gen_random_uuid()::text, '-', '') "
            "|| replace(gen_random_uuid()::text, '-', '')"
        ),
    )
    terminal_cause: Mapped[str | None] = mapped_column(Text, default=None)
    # The provider's own failure message, redacted before it is stored (#3073).
    detail: Mapped[str | None] = mapped_column(Text, default=None)
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    scan_page: Mapped[int] = mapped_column(default=1, server_default="1")
    posted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    comment_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    # Which GitHub comment list ``comment_id`` lives in: issue or PR review.
    comment_list: Mapped[str | None] = mapped_column(Text, default=None)
    rendered_digest: Mapped[str | None] = mapped_column(Text, default=None)
    finalized_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    subject_title: Mapped[str | None] = mapped_column(Text, default=None)
    # NULL: never applied. '': backfilled by 0055, never touch.
    applied_label: Mapped[str | None] = mapped_column(Text, default=None)
    declaration: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    activity: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    refused_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    refusal: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ExecutionRequestPhaseReport(Base):
    """One ``report_progress`` call from the sandbox, on its active request (#3077)."""

    __tablename__ = "execution_request_phase_reports"
    __table_args__ = (
        CheckConstraint(
            "phase ~ '^[a-z][a-z0-9_]{0,63}$'",
            name="execution_request_phase_reports_phase_ck",
        ),
        CheckConstraint(
            "note IS NULL OR length(note) BETWEEN 1 AND 280",
            name="execution_request_phase_reports_note_ck",
        ),
        CheckConstraint(
            "loop_round IS NULL OR loop_round BETWEEN 1 AND 5",
            name="execution_request_phase_reports_round_ck",
        ),
        Index(
            "ix_execution_request_phase_reports_request",
            "execution_request_id",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    execution_request_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.execution_requests.id", ondelete="CASCADE"),
    )
    phase: Mapped[str] = mapped_column(Text)
    note: Mapped[str | None] = mapped_column(Text, default=None)
    loop_round: Mapped[int | None] = mapped_column(default=None)
    reported_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class ExecutionRequestModelUsage(Base):
    """Token usage of one model in one turn of a request, with its estimate (#3223).

    Cost, price source, and price time are all NULL or all set: an unpriced
    model keeps its tokens with no estimate.
    """

    __tablename__ = "execution_request_model_usage"
    __table_args__ = (
        CheckConstraint(
            "role IN ('implementer', 'reviewer')",
            name="execution_request_model_usage_role_ck",
        ),
        CheckConstraint(
            "input_tokens >= 0 AND cached_input_tokens >= 0 "
            "AND cache_write_tokens >= 0 AND output_tokens >= 0",
            name="execution_request_model_usage_tokens_ck",
        ),
        CheckConstraint(
            "(estimated_cost_usd IS NULL AND price_source IS NULL AND price_as_of IS NULL) "
            "OR (estimated_cost_usd IS NOT NULL AND estimated_cost_usd >= 0 "
            "AND price_source IS NOT NULL AND price_as_of IS NOT NULL)",
            name="execution_request_model_usage_price_ck",
        ),
        UniqueConstraint(
            "execution_request_id",
            "turn_id",
            "model",
            "role",
            name="execution_request_model_usage_turn_model_role_key",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    execution_request_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.execution_requests.id", ondelete="CASCADE"),
    )
    turn_id: Mapped[str] = mapped_column(Text)
    model: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text)
    input_tokens: Mapped[int] = mapped_column(BigInteger)
    cached_input_tokens: Mapped[int] = mapped_column(BigInteger)
    cache_write_tokens: Mapped[int] = mapped_column(BigInteger)
    output_tokens: Mapped[int] = mapped_column(BigInteger)
    estimated_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(14, 6), default=None)
    price_source: Mapped[str | None] = mapped_column(Text, default=None)
    price_as_of: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class PublicationReviewReservation(Base):
    """One review origin's claim on the existing publication revision writer."""

    __tablename__ = "publication_review_reservations"
    __table_args__ = (
        CheckConstraint(
            "status IN ('reserved', 'consumed', 'cancelled')",
            name="publication_review_reservations_status_ck",
        ),
        CheckConstraint(
            "version >= 1 AND revision_number >= 1 AND lineage_version >= 1",
            name="publication_review_reservations_versions_ck",
        ),
        Index(
            "uq_reserved_review_per_lineage",
            "lineage_id",
            unique=True,
            postgresql_where=text("status = 'reserved'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    origin_key: Mapped[str] = mapped_column(unique=True)
    lineage_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.thread_publication_lineages.id", ondelete="CASCADE"),
        index=True,
    )
    lineage_version: Mapped[int]
    expected_head_sha: Mapped[str]
    revision_number: Mapped[int]
    binding_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    binding_generation: Mapped[int]
    status: Mapped[str] = mapped_column(default="reserved", server_default="reserved")
    version: Mapped[int] = mapped_column(default=1, server_default="1")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class GitHubReviewDelivery(Base):
    """One authenticated webhook receipt, including ignored actions and aliases."""

    __tablename__ = "github_review_deliveries"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','accepted','ignored','rejected','retryable')",
            name="github_review_deliveries_status_ck",
        ),
        CheckConstraint(
            "version >= 1 AND replay_conflicts >= 0",
            name="github_review_deliveries_version_ck",
        ),
        CheckConstraint(
            "length(body_sha256) = 64",
            name="github_review_deliveries_digest_ck",
        ),
        CheckConstraint(
            "status IN ('pending','accepted') OR reason IS NOT NULL",
            name="github_review_deliveries_reason_ck",
        ),
    )

    delivery_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    event_kind: Mapped[str]
    action: Mapped[str]
    body_sha256: Mapped[str]
    repository_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    installation_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    pr_number: Mapped[int | None] = mapped_column(default=None)
    source_object_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    sender_type: Mapped[str]
    sender_login: Mapped[str | None] = mapped_column(default=None)
    author_association: Mapped[str]
    event_id: Mapped[str | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.github_review_feedback.event_id", ondelete="SET NULL"),
        default=None,
    )
    status: Mapped[str] = mapped_column(default="pending", server_default="pending")
    reason: Mapped[str | None] = mapped_column(default=None)
    version: Mapped[int] = mapped_column(default=1, server_default="1")
    replay_conflicts: Mapped[int] = mapped_column(default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )


class GitHubReviewFeedback(Base):
    """One immutable human feedback identity and its durable enqueue receipt."""

    __tablename__ = "github_review_feedback"
    __table_args__ = (
        CheckConstraint(
            "status IN ('waiting', 'queued', 'reserved', 'settled', 'refused', 'dead_lettered')",
            name="github_review_feedback_status_ck",
        ),
        CheckConstraint("version >= 1", name="github_review_feedback_version_ck"),
        CheckConstraint(
            "enqueue_attempts >= 0", name="github_review_feedback_attempts_ck"
        ),
        CheckConstraint(
            "notice_scan_page >= 1", name="github_review_feedback_notice_scan_page_ck"
        ),
        Index("ix_github_review_feedback_pending", "status", "created_at"),
    )

    event_id: Mapped[str] = mapped_column(primary_key=True)
    delivery_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), unique=True)
    # Keep the semantic tombstone if an operator deletes the old binding or
    # lineage. Recreating one cannot make an old comment executable again.
    lineage_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.thread_publication_lineages.id", ondelete="SET NULL")
    )
    binding_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.agent_channels.id", ondelete="SET NULL")
    )
    binding_generation: Mapped[int]
    agent_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    lineage_version: Mapped[int]
    reservation_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(
            f"{SCHEMA}.publication_review_reservations.id", ondelete="SET NULL"
        ),
        default=None,
    )
    # Only normalized, bounded feedback and a credential-free QueuedTurn; never
    # the unfiltered webhook body, headers, or a GitHub credential.
    feedback: Mapped[dict[str, Any]] = mapped_column(JSONB)
    turn: Mapped[dict[str, Any]] = mapped_column(JSONB)
    traceparent: Mapped[str | None] = mapped_column(default=None)
    status: Mapped[str] = mapped_column(default="waiting", server_default="waiting")
    version: Mapped[int] = mapped_column(default=1, server_default="1")
    enqueue_attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    quota_taken: Mapped[bool] = mapped_column(default=False, server_default="false")
    next_attempt_at: Mapped[datetime | None] = mapped_column(default=None)
    error_code: Mapped[str | None] = mapped_column(default=None)
    stream_id: Mapped[str | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    queued_at: Mapped[datetime | None] = mapped_column(default=None)
    terminal_scan_cursor: Mapped[str | None] = mapped_column(default=None)
    notice_marker: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), default=None)
    notice_scan_page: Mapped[int] = mapped_column(default=1, server_default="1")


class Publication(Base):
    """Private patch state settled by the platform publication reconciler."""

    __tablename__ = "publications"
    __table_args__ = (
        CheckConstraint(
            "status NOT IN ('pending', 'approved', 'launching', 'running') "
            "OR lineage_id IS NOT NULL",
            name="publications_active_lineage_ck",
        ),
        Index("ix_publications_status_lease", "status", "lease_expires_at"),
        Index("ix_publications_deployment_id", "deployment_id"),
        Index(
            "uq_publications_lineage_revision",
            "lineage_id",
            "revision_number",
            unique=True,
        ),
        Index(
            "uq_active_publication_per_lineage",
            "lineage_id",
            unique=True,
            postgresql_where=text("status IN ('pending', 'approved', 'launching', 'running')"),
        ),
        Index(
            "ix_publications_approval_card_delivery",
            "approval_card_reported_at",
            "approval_card_delivery_dead_lettered_at",
            "approval_card_lease_expires_at",
        ),
        Index(
            "ix_publications_resource_cleanup",
            "resource_cleanup_completed_at",
            "resource_cleanup_lease_expires_at",
        ),
        Index(
            "ix_publications_result_delivery",
            "result_reported_at",
            "result_delivery_dead_lettered_at",
            "lease_expires_at",
        ),
        CheckConstraint(
            "branch_prefix IS NULL OR ("
            "char_length(branch_prefix) BETWEEN 2 AND 64 "
            "AND branch_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}/$' "
            "AND branch_prefix NOT LIKE '%..%' "
            "AND branch_prefix NOT LIKE '%.lock/' "
            "AND branch_prefix NOT LIKE '%./')",
            name="publications_branch_prefix_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    approval_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.approvals.id", ondelete="CASCADE"),
        unique=True,
    )
    deployment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.deployments.id", ondelete="CASCADE")
    )
    # Private authorization/history snapshot. New writers derive this from the
    # reply tuple; NULL denotes a successful pre-scoping row whose Approval
    # conversation remains the only honest historical identity.
    workspace_conversation_id: Mapped[str | None] = mapped_column(default=None)
    lineage_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.thread_publication_lineages.id", ondelete="SET NULL"),
        default=None,
    )
    execution_request_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.execution_requests.id", ondelete="SET NULL"),
        default=None,
    )
    revision_number: Mapped[int | None] = mapped_column(default=None)
    expected_prior_head: Mapped[str | None] = mapped_column(default=None)
    repo_full_name: Mapped[str]
    status: Mapped[str] = mapped_column(server_default="pending")
    # Snapshotted from the agent policy at creation. Human policy stores false
    # and NULL. The worker enforces these bounds and does not reread the agent.
    open_as_draft: Mapped[bool] = mapped_column(default=False, server_default="false")
    branch_prefix: Mapped[str | None] = mapped_column(default=None)
    version: Mapped[int] = mapped_column(server_default="1", default=1)
    base_sha: Mapped[str]
    # Deliberately excluded from every public DTO. Terminal retention clears
    # these bytes while preserving the audit/result metadata.
    patch_bytes: Mapped[bytes | None] = mapped_column(LargeBinary, default=None)
    changed_paths: Mapped[list[str]] = mapped_column(JSONB)
    observed_title_sha256: Mapped[str | None] = mapped_column(default=None)
    observed_body_sha256: Mapped[str | None] = mapped_column(default=None)
    metadata_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    title: Mapped[str]
    body: Mapped[str] = mapped_column(Text)
    reply_kind: Mapped[str]
    reply_channel: Mapped[str]
    reply_placeholder: Mapped[str | None] = mapped_column(default=None)
    reply_endpoint: Mapped[str | None] = mapped_column(default=None)
    reply_adapter: Mapped[str | None] = mapped_column(default=None)
    # Durable initial approval-card outbox. Its lease/version are separate from
    # publication mutation, while claim_next gates Job creation on delivery so
    # even an immediate CLI approval cannot race ahead of the required card.
    approval_card_reported_at: Mapped[datetime | None] = mapped_column(default=None)
    approval_card_delivery_started_at: Mapped[datetime | None] = mapped_column(default=None)
    approval_card_delivery_attempts: Mapped[int] = mapped_column(server_default="0", default=0)
    approval_card_version: Mapped[int] = mapped_column(server_default="1", default=1)
    approval_card_delivery_error: Mapped[str | None] = mapped_column(Text, default=None)
    approval_card_delivery_dead_lettered_at: Mapped[datetime | None] = mapped_column(default=None)
    approval_card_lease_owner: Mapped[str | None] = mapped_column(default=None)
    approval_card_lease_expires_at: Mapped[datetime | None] = mapped_column(default=None)
    # Resource cleanup is an unbounded durable obligation, separate from the
    # bounded human-facing result outbox. A Slack outage can dead-letter its
    # report; credentials and publication resources can never be abandoned.
    resource_cleanup_completed_at: Mapped[datetime | None] = mapped_column(default=None)
    resource_cleanup_error: Mapped[str | None] = mapped_column(Text, default=None)
    resource_cleanup_version: Mapped[int] = mapped_column(server_default="1", default=1)
    resource_cleanup_lease_owner: Mapped[str | None] = mapped_column(default=None)
    resource_cleanup_lease_expires_at: Mapped[datetime | None] = mapped_column(default=None)
    lease_owner: Mapped[str | None] = mapped_column(default=None)
    lease_expires_at: Mapped[datetime | None] = mapped_column(default=None)
    result_url: Mapped[str | None] = mapped_column(default=None)
    error: Mapped[str | None] = mapped_column(Text, default=None)
    # Terminalization and credential/resource cleanup happen before reply
    # delivery. These fields form the durable result outbox so a transient
    # adapter failure cannot resurrect publication work or retain patch bytes.
    result_reported_at: Mapped[datetime | None] = mapped_column(default=None)
    # A terminal publication does not release its thread fence until the
    # platform-authored outcome is durable in the transcript the next sandbox
    # rehydrates. Result delivery to Slack is a separate, independently retrying
    # obligation and must not stand in for this acknowledgement.
    outcome_history_ready_at: Mapped[datetime | None] = mapped_column(default=None)
    result_delivery_attempts: Mapped[int] = mapped_column(server_default="0", default=0)
    result_delivery_error: Mapped[str | None] = mapped_column(Text, default=None)
    result_delivery_dead_lettered_at: Mapped[datetime | None] = mapped_column(default=None)
    reconcile_attempts: Mapped[int] = mapped_column(server_default="0", default=0)
    reconcile_dead_lettered_at: Mapped[datetime | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    terminal_at: Mapped[datetime | None] = mapped_column(default=None)

    approval: Mapped[Approval] = relationship(back_populates="publication")
    lineage: Mapped[ThreadPublicationLineage | None] = relationship(back_populates="publications")

    @property
    def lineage_base_sha(self) -> str | None:
        return self.lineage.base_sha if self.lineage is not None else None

    @property
    def lineage_head_sha(self) -> str | None:
        return self.lineage.head_sha if self.lineage is not None else None

    @property
    def lineage_state(self) -> str | None:
        return self.lineage.status if self.lineage is not None else None

    @property
    def lineage_version(self) -> int | None:
        return self.lineage.version if self.lineage is not None else None

    @property
    def branch(self) -> str | None:
        return self.lineage.branch if self.lineage is not None else None

    @property
    def pr_number(self) -> int | None:
        return self.lineage.pr_number if self.lineage is not None else None

    @property
    def pr_url(self) -> str | None:
        return self.lineage.pr_url if self.lineage is not None else None


class CredentialRedemptionAuditEntry(Base):
    """Credential-boundary audit containing names and outcomes, never material."""

    __tablename__ = "credential_redemption_audit_entries"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    purpose: Mapped[str]
    outcome: Mapped[str]
    deployment_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.deployments.id", ondelete="SET NULL"),
        default=None,
    )
    publication_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.publications.id", ondelete="SET NULL"),
        default=None,
    )
    repo_full_name: Mapped[str | None] = mapped_column(default=None)
    detail: Mapped[str | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class ApprovalAuditEntry(Base):
    """The platform audit log for approvals (#247, ADR-0010).

    One row per authorization-relevant event on an approval: a resolution that
    won, a denied attempt, an expiry. Each row snapshots WHO acted, from where,
    and the authorizer verdict that counted (or refused) them -- the answer to
    "who resolved, and why they counted" that a black-box approval cannot give.
    Append-only: rows are written by the resolve endpoint and never updated.
    """

    __tablename__ = "approval_audit_entries"
    __table_args__ = (
        CheckConstraint(
            "principal_kind IS NULL OR principal_kind IN "
            "('chat', 'console', 'operator', 'adapter', 'platform')",
            name="approval_audit_principal_kind_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    approval_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.approvals.id", ondelete="CASCADE"), index=True
    )
    # What happened: resolved / denied / race_lost / expired / reraise_refused
    # (a re-raise of this rejected approval refused, #2885).
    action: Mapped[str]
    actor: Mapped[str]
    actor_channel: Mapped[str | None] = mapped_column(default=None)
    # The proof attached to this actor (ADR-0106). Historical and system rows
    # honestly retain NULL/false rather than being retro-labelled.
    principal_kind: Mapped[str | None] = mapped_column(default=None)
    authenticated: Mapped[bool] = mapped_column(server_default="false", default=False)
    # The adapter that transported an `adapter` principal's decision (ADR-0154);
    # `actor` is then the sender it authenticated. NULL for every other kind.
    principal_subject: Mapped[str | None] = mapped_column(default=None)
    # The decision the actor attempted (approved/rejected).
    decision: Mapped[str]
    # The authorizer snapshot: which implementation decided, its verdict, and
    # its stated reason at the time of the attempt.
    authorizer: Mapped[str]
    authorized: Mapped[bool]
    reason: Mapped[str | None] = mapped_column(default=None)
    # The membership facts the authorizer decided on (#420): the group ID and
    # the actor's verdict, the allowlist that counted, or the channels compared.
    # Nullable because writers that make no membership decision (the expiry
    # sweeper) must leave it NULL rather than fabricate one, and because rows
    # written before this column existed have none.
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


# @spec AUTOMATED-REMEDIATION-14 (executor amendment E1): the closed sets the
# ledger checks enforce. ``authority_kind`` names what permitted a
# platform-executed call; ``actor_kind`` who acted; the outcome is written by
# verification. Revision 0089 carries the same literals.
AUTHORITY_KINDS: tuple[str, ...] = (
    "undo_ruling",
    "capability_probe",
    "policy",
    "approval",
    "qualification",
)
ACTOR_KINDS: tuple[str, ...] = ("model_turn", "policy", "approval", "undo_ruling")
VERIFICATION_OUTCOMES: tuple[str, ...] = (
    "verified",
    "not-recovered",
    "verifier-unavailable",
    "superseded",
)


def _sql_values(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


class AgentAction(Base):
    """One thing an agent did to the world, and what it takes to put it back.

    Curie already classified every tool absent from a harness-declared read-only
    allowlist as side-effecting and reduced the whole stream to one boolean, for
    one purpose: refusing to auto-retry. This is that same classification
    recorded rather than reduced (ADR-0117).

    Shaped after ``Approval`` because the needs are the same ones that table
    already answers: routing by conversation, a lifecycle resolved once, and an
    audit trail beside it. ``dedupe_key`` (the triggering event id and the call
    id) makes record creation idempotent under at-least-once redelivery, exactly
    as it does there.

    One CALL is one row. The ACI emits an opening frame when the call is made and
    a closing frame when its result arrives, joined on ``call_id``; the second
    completes this row rather than minting another.
    """

    __tablename__ = "agent_actions"
    __table_args__ = (
        # @spec ACTION-EXECUTOR-2: the target of the composite key that binds an
        # execution to its subject action's agent. ``id`` alone is already
        # unique, so this adds no restriction on the ledger itself.
        UniqueConstraint("id", "agent_id", name="uq_agent_actions_id_agent_id"),
        # @spec AUTOMATED-REMEDIATION-14: closed domains, as database checks.
        CheckConstraint(
            f"authority_kind IS NULL OR authority_kind IN ({_sql_values(AUTHORITY_KINDS)})",
            name="agent_actions_authority_kind_ck",
        ),
        CheckConstraint(
            f"actor_kind IS NULL OR actor_kind IN ({_sql_values(ACTOR_KINDS)})",
            name="agent_actions_actor_kind_ck",
        ),
        CheckConstraint(
            "verification_outcome IS NULL OR verification_outcome IN "
            f"({_sql_values(VERIFICATION_OUTCOMES)})",
            name="agent_actions_verification_outcome_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Nullable for the same reason ``Approval.agent_id`` is: a run without a
    # deployment binding still acts on the world and still owes a record.
    agent_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        index=True,
        default=None,
    )
    conversation_id: Mapped[str] = mapped_column(index=True)
    # The harness's own id for the call, carried on both ACI frames. The join
    # key, not a display value.
    call_id: Mapped[str]
    tool: Mapped[str]
    # What the call was made with, and what the tool answered. ``result`` is
    # present only for a structured reply: a connector that answers in prose has
    # none, because guessing structure out of a sentence is how a restore ends up
    # acting on a guess.
    arguments: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # The state the connector read immediately before it wrote, and the resource
    # it wrote to. These two are what a restore replays; without either, there is
    # nothing to put back or nowhere to put it.
    prior_state: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    target: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # What the call LEFT, reported by the same reply that reported what it read.
    # The world-moved check compares the live resource against this, never
    # against ``prior_state`` -- that is where the resource came from, not where
    # the action put it. It cannot be derived from ``arguments``: a PATCH's
    # result is not its request body, and deriving it is the mapping-DSL
    # approach ADR-0117 rejects.
    post_state: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # The frame's human-readable note. On a record that is not undoable this is
    # the stated reason the receipt shows instead of a control.
    detail: Mapped[str | None] = mapped_column(default=None)
    # The approval that gated the call, when one did (ADR-0117 decision 3). NULL
    # means the tool was not gated, and an undo is then not gated either.
    #
    # Deliberately NOT a foreign key. This is the record of what authorization
    # the forward action required, and a sweeper deleting the approval row must
    # not silently downgrade a gated action to an ungated one. An id whose
    # approval can no longer be read fails closed at the undo instead.
    gate_approval_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), default=None)
    status: Mapped[str] = mapped_column(server_default=ActionStatus.pending, index=True)
    dedupe_key: Mapped[str] = mapped_column(unique=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    # When the closing frame arrived, and when a restore was performed. Both are
    # written by later slices; they live here because they are lifecycle of this
    # row, and a two-column migration later buys nothing.
    completed_at: Mapped[datetime | None] = mapped_column(default=None)
    # Written only when a restore execution is CONFIRMED (ACTION-EXECUTOR-11);
    # rows from before the executor may carry the ruling-time claim instead.
    undone_at: Mapped[datetime | None] = mapped_column(default=None)
    undone_by: Mapped[str | None] = mapped_column(default=None)
    # @spec ACTION-EXECUTOR-11: what a pinned, sealed restore needs beyond the
    # envelope in ``prior_state``. ``post_version`` is the opaque version the
    # call left, compared with ``observe_version`` before any restore;
    # ``connector`` and ``connector_digest`` pin the image the restore must run
    # under; ``authority_kind`` and ``authority_ref`` name what permitted a
    # platform-executed forward call (shared with #4068). All NULL on rows
    # written before the executor, which are therefore never undoable.
    post_version: Mapped[str | None] = mapped_column(Text, default=None)
    connector: Mapped[str | None] = mapped_column(Text, default=None)
    connector_digest: Mapped[str | None] = mapped_column(Text, default=None)
    authority_kind: Mapped[str | None] = mapped_column(Text, default=None)
    authority_ref: Mapped[str | None] = mapped_column(Text, default=None)
    # @spec AUTOMATED-REMEDIATION-14: a remediation's provenance and outcome.
    # ``delivery_event_id`` is the protected delivery the action was nominated
    # from and ``nomination_id`` the nomination (not a foreign key, like
    # ``gate_approval_id``: the record outlives the nomination row).
    # ``actor_kind`` says who acted: ``model_turn`` for a call a turn recorded,
    # ``policy`` or ``approval`` for a platform-executed remediation. All NULL
    # on rows written before revision 0089.
    delivery_event_id: Mapped[str | None] = mapped_column(String(256), default=None)
    nomination_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), default=None)
    verification_outcome: Mapped[str | None] = mapped_column(Text, default=None)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    actor_kind: Mapped[str | None] = mapped_column(Text, default=None)

    def restore_record_refusal(self) -> str | None:
        """The ruling code for the first record ingredient missing, or None.

        @spec ACTION-EXECUTOR-11. The record-level half of ``undoable``,
        derived and never stored: not yet undone, succeeded, a valid sealed
        envelope in ``prior_state`` (ACTION-EXECUTOR-9), attributed to an agent,
        a ``post_version``, a ``target``, and a ``connector`` with its
        ``connector_digest``. A cleartext ``prior_state`` is history, not a
        snapshot, so a legacy row is ``refused_unsealed``. ``post_state`` is no
        longer read for a sealed record.

        The other half (no live restore, capability and key custody) needs the
        database and the in-force bundle. ``curie_api.action_undoable`` combines
        both and is the only place ``undoable`` and its refusal are answered.
        """

        if self.undone_at is not None:
            return "refused_already_undone"
        if self.status != ActionStatus.succeeded:
            return "refused_unsuccessful"
        # A record without a sealed snapshot holds nothing to restore under any
        # agent, so that is the most specific reason and comes first.
        if not is_sealed_envelope(self.prior_state):
            return "refused_unsealed"
        if self.agent_id is None:
            return "refused_no_agent"
        if not self.post_version:
            return "refused_unversioned"
        if self.target is None:
            return "refused_irreversible"
        if not self.connector or not self.connector_digest:
            return "refused_no_digest"
        return None

    @property
    def holds_restore_record(self) -> bool:
        """Whether every record-level ingredient is present (ACTION-EXECUTOR-11)."""

        return self.restore_record_refusal() is None


class ActionAuditEntry(Base):
    """The platform audit log for actions (ADR-0117), append-only.

    One row per authorization-relevant event on a recorded action: an undo that
    ran, an undo refused because the world had moved, an undo refused because the
    actor could not have permitted the forward change. A refusal is as much of a
    record as a restore -- more, since a refused undo leaves no trace anywhere
    else.
    """

    __tablename__ = "action_audit_entries"
    __table_args__ = (
        CheckConstraint(
            f"actor_kind IS NULL OR actor_kind IN ({_sql_values(ACTOR_KINDS)})",
            name="action_audit_entries_actor_kind_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    action_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agent_actions.id", ondelete="CASCADE"), index=True
    )
    # What happened: undone / refused_conflict / refused_unauthorized.
    action: Mapped[str]
    actor: Mapped[str]
    actor_channel: Mapped[str | None] = mapped_column(default=None)
    # The authorizer snapshot, as on an approval: which implementation decided,
    # its verdict, and its stated reason at the time of the attempt.
    authorizer: Mapped[str]
    authorized: Mapped[bool]
    reason: Mapped[str | None] = mapped_column(default=None)
    # For a conflict refusal, the two states that disagreed. Naming both is the
    # point: an operator has to see that their manual fix is what stopped it.
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    # @spec AUTOMATED-REMEDIATION-14: who acted, from ``ACTOR_KINDS``. A policy
    # actor is ``policy`` with the policy reference as ``actor``, never an
    # empty human field. NULL on rows written before revision 0089.
    actor_kind: Mapped[str | None] = mapped_column(Text, default=None)


class ExecutionKind(enum.StrEnum):
    """What an action execution runs (ACTION-EXECUTOR-2)."""

    restore = "restore"
    forward = "forward"
    probe = "probe"
    # @spec AUTOMATED-REMEDIATION-12 (executor amendment E2): one remediation
    # sample, which never enters ``dispatched``.
    read = "read"


class ExecutionState(enum.StrEnum):
    """Lifecycle of one action execution (ACTION-EXECUTOR-17).

    ``refused`` is a provable non-write and releases the action; ``confirmed``,
    ``failed`` and ``indeterminate`` are terminal and may have written, so a
    restore in any state but ``refused`` holds its action.
    """

    requested = "requested"
    claimed = "claimed"
    dispatched = "dispatched"
    confirmed = "confirmed"
    failed = "failed"
    indeterminate = "indeterminate"
    refused = "refused"


def _sql_in(values: type[enum.StrEnum]) -> str:
    return ", ".join(f"'{member.value}'" for member in values)


class ActionExecution(Base):
    """One call the platform runs without a model (ACTION-EXECUTOR-2).

    @spec ACTION-EXECUTOR-2. A restore of a recorded action, a forward action
    whose authority its owner verified, or a read-only capability probe, each
    run under the target connector's own binding. Created only by the undo
    ruling, the forward creation function and the probe route
    (ACTION-EXECUTOR-1). ``idempotency_key`` is unique within one agent, so a
    replayed creation adopts that agent's existing row and no other's, and a
    composite key ties ``agent_id`` to the subject action's agent. At most one
    restore that is not ``refused`` may name one action, enforced by a partial
    unique index rather than a writer.
    ``outcome`` carries version strings, a key identifier and codes only, never
    an envelope, a state or a result.
    """

    __tablename__ = "action_executions"
    __table_args__ = (
        CheckConstraint(f"kind IN ({_sql_in(ExecutionKind)})", name="action_executions_kind_ck"),
        CheckConstraint(
            f"state IN ({_sql_in(ExecutionState)})", name="action_executions_state_ck"
        ),
        # @spec AUTOMATED-REMEDIATION-14 (executor amendment E1).
        CheckConstraint(
            f"authority_kind IN ({_sql_values(AUTHORITY_KINDS)})",
            name="action_executions_authority_kind_ck",
        ),
        # A replayed creation adopts the existing row on (agent_id, key), so one
        # agent's key can never adopt another agent's execution.
        UniqueConstraint(
            "agent_id", "idempotency_key", name="uq_action_executions_agent_idempotency_key"
        ),
        # An execution always runs under the agent whose action it concerns. An
        # action with no agent matches no pair, so it can never be a subject.
        ForeignKeyConstraint(
            ["subject_action_id", "agent_id"],
            [f"{SCHEMA}.agent_actions.id", f"{SCHEMA}.agent_actions.agent_id"],
            ondelete="CASCADE",
            name="fk_action_executions_subject_agent",
        ),
        Index(
            "uq_action_executions_live_restore",
            "subject_action_id",
            unique=True,
            postgresql_where=text("kind = 'restore' AND state <> 'refused'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    kind: Mapped[str] = mapped_column(Text)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), index=True
    )
    connector: Mapped[str] = mapped_column(Text)
    # The upstream tool name; ``restore`` for a restore, NULL for a probe.
    tool: Mapped[str | None] = mapped_column(Text, default=None)
    # Restore: the action put back. Forward: the record created at dispatch.
    subject_action_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), default=None
    )
    arguments_sha256: Mapped[str | None] = mapped_column(Text, default=None)
    forward_arguments: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    connector_digest: Mapped[str] = mapped_column(Text)
    authority_kind: Mapped[str] = mapped_column(Text)
    authority_ref: Mapped[str] = mapped_column(Text)
    requested_by: Mapped[str | None] = mapped_column(Text, default=None)
    idempotency_key: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(
        Text, server_default=ExecutionState.requested, index=True
    )
    refusal_code: Mapped[str | None] = mapped_column(Text, default=None)
    failure_code: Mapped[str | None] = mapped_column(Text, default=None)
    attempt: Mapped[int] = mapped_column(Integer, server_default="0", default=0)
    lease_owner: Mapped[str | None] = mapped_column(Text, default=None)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    dispatched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    outcome: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)
    # @spec AUTOMATED-REMEDIATION-12 (executor amendment E9): handed out only
    # once due; NULL is due.
    not_before: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    # A read's RFC 6901 pointer, bound by its producer with tool and arguments.
    pointer: Mapped[str | None] = mapped_column(Text, default=None)
    # A read's ``{"sample", "value"}`` as reported, or the API's ``skipped``.
    sample: Mapped[dict[str, Any] | None] = mapped_column(JSONB, default=None)


class ConnectorCapability(Base):
    """Whether one connector image can restore, as its probe observed it.

    @spec ACTION-EXECUTOR-13. Keyed on the agent, connector and image digest:
    a digest's tool list is a property of the image, so one probe answers for
    every action recorded under it, including actions recorded before the probe
    completed. Key custody depends on the version's declarations, not the image,
    and is deliberately not stored here (ACTION-EXECUTOR-16).
    """

    __tablename__ = "connector_capabilities"

    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), primary_key=True
    )
    connector: Mapped[str] = mapped_column(Text, primary_key=True)
    digest: Mapped[str] = mapped_column(Text, primary_key=True)
    restore_capable: Mapped[bool]
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ChannelCanvasEdit(Base):
    """One agent edit of one canvas cell (ADR 0200), append and settle.

    The row is committed ``attempted`` before Slack is called, then settled
    ``applied`` or ``failed`` only when Slack's answer is definite. A row left
    ``attempted`` means the edit may have applied. ``before_text`` and
    ``after_text`` are never logged or rendered.
    """

    __tablename__ = "channel_canvas_edits"
    __table_args__ = (
        CheckConstraint(
            "status IN ('attempted', 'applied', 'failed')",
            name="channel_canvas_edits_status_ck",
        ),
        Index("ix_channel_canvas_edits_canvas_created", "canvas_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), index=True
    )
    deployment_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    turn: Mapped[str] = mapped_column(Text)
    kind: Mapped[str] = mapped_column(Text)
    channel_address: Mapped[str] = mapped_column(Text)
    canvas_id: Mapped[str] = mapped_column(Text)
    section_id: Mapped[str] = mapped_column(Text)
    before_text: Mapped[str] = mapped_column(Text)
    after_text: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    error_code: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class WorkflowStateEntry(Base):
    """Durable, agent-scoped key/value state (#23, first slice).

    Cross-turn business state (a pending-approvals map, a dedupe seen-set) has
    nowhere durable to live today: sandboxes do not survive suspend, so agents
    keep it in-process and lose it on restart. This is a small scoped store --
    namespace + key per agent, an arbitrary-JSON value, and a monotonic
    ``version`` for compare-and-set. Backed by Postgres JSONB (no new datastore).
    Exposing it to bundle code via an auto-mounted MCP server is a later slice;
    this lands the store and its HTTP API.
    """

    __tablename__ = "workflow_state_entries"
    __table_args__ = (
        UniqueConstraint(
            "agent_id",
            "binding_scope",
            "namespace",
            "key",
            name="uq_state_agent_scope_ns_key",
            postgresql_nulls_not_distinct=True,
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE")
    )
    # NULL is one agent-wide shared identity: for general state when the owning
    # agent has `memory=True`, and for the reserved `memory` namespace's
    # agent-wide rows. The `transcript` namespace is not stored in this table at
    # all: it lives in `thread_transcripts` (ADR-0170), keyed by its own
    # `binding_scope`. The binding's own `"{kind}:{address}"` is the isolated identity
    # for general state when `memory=False` (#1525 follow-up). Minted into the
    # worker's `state.app`/`state` token per turn from the agent's CURRENT
    # `memory` value, never read back off this column -- the column only picks
    # which row a request lands on. The NULLS-NOT-DISTINCT unique key makes the
    # shared identity singular while distinct non-NULL scopes remain isolated.
    binding_scope: Mapped[str | None] = mapped_column(default=None)
    namespace: Mapped[str]
    key: Mapped[str]
    # Any JSON value: an object (a pending-approvals map), an array (a log
    # grown by append, #248), or a scalar. JSONB stores all of them.
    value: Mapped[Any] = mapped_column(JSONB)
    # Monotonic per-entry counter for compare-and-set: a put may pass the version
    # it last read, and the write is rejected if the stored version moved on.
    version: Mapped[int] = mapped_column(default=1)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())



class ThreadTranscript(Base):
    """One thread's conversation transcript (ADR-0170, #3070).

    Moved out of ``workflow_state_entries``: that store caps a whole (agent,
    namespace), so every thread an agent ever ran shared one transcript budget
    and a busy factory agent stopped for good once it filled. A transcript is
    capped per thread here (``transcript_max_thread_bytes``) with no agent-wide
    cap, and it is deleted when its WorkItem reaches a terminal state or, for a
    thread with no WorkItem, once ``expires_at`` passes. The state API keeps
    serving it under ``/state/transcript/<thread_key>``.
    """

    __tablename__ = "thread_transcripts"
    __table_args__ = (
        UniqueConstraint(
            "agent_id",
            "binding_scope",
            "thread_key",
            name="uq_thread_transcripts_agent_scope_thread",
            postgresql_nulls_not_distinct=True,
        ),
        Index("ix_thread_transcripts_expires_at", "expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE")
    )
    # Same partition key as ``WorkflowStateEntry.binding_scope``; NULL is the
    # agent-wide identity every runner transcript uses today.
    binding_scope: Mapped[str | None] = mapped_column(default=None)
    # The worker's scoped thread key, which is also ``WorkItem.conversation_id``.
    thread_key: Mapped[str]
    value: Mapped[Any] = mapped_column(JSONB)
    version: Mapped[int] = mapped_column(default=1)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class ThreadAttachmentRef(Base):
    """One file a thread's agent was given (ADR 0205, #4079).

    Keyed exactly like ``ThreadTranscript`` and removed with it
    (``curie_api.thread_attachments``). Only the worker writes it, through the
    internal routes. It records the channel's file id and the route it came
    from, never an endpoint, a URL or bytes. ``seq`` is the arrival order;
    ``disk_name`` is fixed when recorded and unique in the thread, so a path a
    notice named is the path on every later boot.
    """

    __tablename__ = "thread_attachment_refs"
    __table_args__ = (
        UniqueConstraint(
            "agent_id",
            "binding_scope",
            "thread_key",
            "event_id",
            "file_id",
            name="uq_thread_attachment_refs_event_file",
            postgresql_nulls_not_distinct=True,
        ),
        UniqueConstraint(
            "agent_id",
            "binding_scope",
            "thread_key",
            "disk_name",
            name="uq_thread_attachment_refs_disk_name",
            postgresql_nulls_not_distinct=True,
        ),
        Index(
            "ix_thread_attachment_refs_thread_seq",
            "agent_id",
            "binding_scope",
            "thread_key",
            "seq",
        ),
        Index("ix_thread_attachment_refs_expires_at", "expires_at"),
        # The orphan sweep runs on every transcript write, scoped to one agent.
        Index("ix_thread_attachment_refs_agent_expires_at", "agent_id", "expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    seq: Mapped[int] = mapped_column(BigInteger, Identity(always=True))
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE")
    )
    binding_scope: Mapped[str | None] = mapped_column(Text, default=None)
    thread_key: Mapped[str] = mapped_column(Text)
    event_id: Mapped[str] = mapped_column(Text)
    file_id: Mapped[str] = mapped_column(Text)
    ordinal: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(Text)
    disk_name: Mapped[str] = mapped_column(Text)
    mime_type: Mapped[str | None] = mapped_column(Text, default=None)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, default=None)
    sha256: Mapped[str] = mapped_column(Text)
    route_kind: Mapped[str] = mapped_column(Text)
    route_adapter: Mapped[str | None] = mapped_column(Text, default=None)
    route_identity: Mapped[str] = mapped_column(Text)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ConsoleSession(Base):
    """One console login: the code that establishes it and the session it becomes.

    ADR-0083. The console authenticates with a server-managed, revocable session
    instead of holding the platform key in browser code. A row is created when the
    CLI mints a login code and completed when the browser exchanges that code for a
    session token.

    Only HASHES of the code and the token are stored, so reading this table cannot
    replay a session -- the same reason `Approval` does not store credentials. And
    revocation is `revoked_at`, a column write: a durable row a human can kill,
    rather than a self-contained signed token that stays valid until it expires.
    That distinction is why ADR-0083 rejected a stateless JWT-shaped token.
    """

    __tablename__ = "console_sessions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Administrator-selected at login-code mint and immutable thereafter. NULL
    # preserves pre-ADR-0106 sessions, which cannot resolve approvals.
    subject: Mapped[str | None] = mapped_column(default=None)
    # SHA-256 hex of the single-use login code. Unique so a hash collision or a
    # duplicate mint cannot produce two rows one code could satisfy.
    login_code_hash: Mapped[str] = mapped_column(unique=True, index=True)
    login_code_expires_at: Mapped[datetime]
    # Set at exchange, so NULL means "minted, never redeemed".
    session_token_hash: Mapped[str | None] = mapped_column(default=None, unique=True, index=True)
    session_expires_at: Mapped[datetime | None] = mapped_column(default=None)
    # Stamped at exchange; its presence is what makes the code single-use.
    consumed_at: Mapped[datetime | None] = mapped_column(default=None)
    revoked_at: Mapped[datetime | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class ScheduleControl(Base):
    """Operator pause state for one agent and named cron hook."""

    __tablename__ = "schedule_controls"

    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), primary_key=True
    )
    name: Mapped[str] = mapped_column(String, primary_key=True)
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    resume_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")


class HookSourcePolicy(Base):
    """@spec PROTECTED-HOOK-SOURCE-1."""

    __tablename__ = "hook_source_policies"
    __table_args__ = (
        CheckConstraint("generation > 0", name="hook_source_policies_generation_ck"),
        CheckConstraint(
            "mode IN ('protected', 'ordinary')", name="hook_source_policies_mode_ck"
        ),
        CheckConstraint(
            "(mode = 'protected' AND tool_access IS NOT NULL "
            "AND tool_access = 'read-only' AND runtime_id IS NOT NULL "
            "AND qualification_id IS NOT NULL AND bundle_digest IS NOT NULL) "
            "OR (mode = 'ordinary' AND tool_access IS NULL AND runtime_id IS NULL "
            "AND qualification_id IS NULL AND bundle_digest IS NULL)",
            name="hook_source_policies_policy_ck",
        ),
    )

    agent_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        primary_key=True,
    )
    hook: Mapped[str] = mapped_column(String(63), primary_key=True)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    operation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    tool_access: Mapped[str | None] = mapped_column(String, nullable=True)
    runtime_id: Mapped[str | None] = mapped_column(String, nullable=True)
    qualification_id: Mapped[str | None] = mapped_column(String, nullable=True)
    bundle_digest: Mapped[str | None] = mapped_column(String, nullable=True)
    legacy_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class HookSourceOperation(Base):
    """@spec PROTECTED-HOOK-SOURCE-10."""

    __tablename__ = "hook_source_operations"
    __table_args__ = (
        CheckConstraint("generation > 0", name="hook_source_operations_generation_ck"),
        CheckConstraint(
            "status IN ('pending', 'committed')", name="hook_source_operations_status_ck"
        ),
        CheckConstraint(
            "intent_sha256 ~ '^[0-9a-f]{64}$'", name="hook_source_operations_intent_ck"
        ),
        UniqueConstraint(
            "agent_id", "hook", "generation", name="uq_hook_source_operation_generation"
        ),
    )

    agent_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        primary_key=True,
    )
    hook: Mapped[str] = mapped_column(String(63), primary_key=True)
    operation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    intent_sha256: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class RemediationPolicy(Base):
    """The current remediation policy generation of one bound hook.

    @spec AUTOMATED-REMEDIATION-2.
    """

    __tablename__ = "remediation_policies"
    __table_args__ = (
        CheckConstraint("generation > 0", name="remediation_policies_generation_ck"),
        CheckConstraint("active OR NOT armed", name="remediation_policies_armed_ck"),
    )

    agent_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        primary_key=True,
    )
    hook: Mapped[str] = mapped_column(String(63), primary_key=True)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    operation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    armed: Mapped[bool] = mapped_column(nullable=False)
    active: Mapped[bool] = mapped_column(nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class RemediationPolicyGeneration(Base):
    """One immutable remediation policy generation, kept while the agent exists.

    @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3.
    """

    __tablename__ = "remediation_policy_generations"
    __table_args__ = (
        CheckConstraint("generation > 0", name="remediation_policy_generations_generation_ck"),
        CheckConstraint(
            "intent_sha256 ~ '^[0-9a-f]{64}$'", name="remediation_policy_generations_intent_ck"
        ),
        CheckConstraint("active OR NOT armed", name="remediation_policy_generations_armed_ck"),
        CheckConstraint(
            "length(btrim(bound_by)) > 0", name="remediation_policy_generations_bound_by_ck"
        ),
        UniqueConstraint(
            "agent_id", "hook", "operation_id", name="uq_remediation_policy_generation_operation"
        ),
    )

    agent_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        primary_key=True,
    )
    hook: Mapped[str] = mapped_column(String(63), primary_key=True)
    generation: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    operation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    intent_sha256: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    document: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    armed: Mapped[bool] = mapped_column(nullable=False)
    active: Mapped[bool] = mapped_column(nullable=False)
    bound_by: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class RemediationNominationSubmission(Base):
    """The one accepted nomination submission of a protected event.

    Keyed on the event so the first accepted submission wins; ``block_sha256``
    decides a byte-identical replay from a ``nomination_conflict``.
    @spec AUTOMATED-REMEDIATION-6.
    """

    __tablename__ = "remediation_nomination_submissions"
    __table_args__ = (
        CheckConstraint(
            "block_sha256 ~ '^[0-9a-f]{64}$'", name="remediation_nomination_submissions_digest_ck"
        ),
    )

    event_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), nullable=False
    )
    hook: Mapped[str] = mapped_column(String(63), nullable=False)
    block_sha256: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class RemediationNomination(Base):
    """One nominated action of a protected turn, or one malformed block.

    @spec AUTOMATED-REMEDIATION-7 @spec AUTOMATED-REMEDIATION-8.
    """

    __tablename__ = "remediation_nominations"
    __table_args__ = (
        CheckConstraint(
            "state IN ('received', 'refused', 'precondition_pending', 'admitted', "
            "'approval_requested', 'approved', 'rejected', 'expired', 'executing', "
            "'verifying', 'finished')",
            name="remediation_nominations_state_ck",
        ),
        CheckConstraint(
            "refusal_code IS NULL OR refusal_code IN ('nomination_malformed', 'unknown_action', "
            "'nomination_duplicate', 'arguments_schema_mismatch', 'agent_stopped')",
            name="remediation_nominations_refusal_ck",
        ),
        CheckConstraint(
            "(state = 'refused') = (refusal_code IS NOT NULL)",
            name="remediation_nominations_refused_ck",
        ),
        CheckConstraint(
            "refusal_code = 'nomination_malformed' OR "
            "(action IS NOT NULL AND arguments IS NOT NULL AND arguments_sha256 IS NOT NULL)",
            name="remediation_nominations_action_ck",
        ),
        CheckConstraint(
            "kind IS NULL OR kind IN ('remediate', 'prevent', 'tune')",
            name="remediation_nominations_kind_ck",
        ),
        CheckConstraint(
            "verification_outcome IS NULL OR verification_outcome IN "
            "('verified', 'not-recovered', 'verifier-unavailable', 'superseded')",
            name="remediation_nominations_outcome_ck",
        ),
        CheckConstraint(
            "arguments_sha256 IS NULL OR arguments_sha256 ~ '^[0-9a-f]{64}$'",
            name="remediation_nominations_digest_ck",
        ),
        CheckConstraint(
            "admitted_generation IS NULL OR admitted_generation > 0",
            name="remediation_nominations_admitted_ck",
        ),
        CheckConstraint(
            "current_generation IS NULL OR current_generation > 0",
            name="remediation_nominations_current_ck",
        ),
        Index("ix_remediation_nominations_event", "event_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"), nullable=False
    )
    hook: Mapped[str] = mapped_column(String(63), nullable=False)
    event_id: Mapped[str] = mapped_column(
        String(256),
        ForeignKey(f"{SCHEMA}.remediation_nomination_submissions.event_id", ondelete="CASCADE"),
        nullable=False,
    )
    admitted_generation: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    current_generation: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    action: Mapped[str | None] = mapped_column(Text, nullable=True)
    kind: Mapped[str | None] = mapped_column(Text, nullable=True)
    arguments: Mapped[str | None] = mapped_column(Text, nullable=True)
    arguments_sha256: Mapped[str | None] = mapped_column(CHAR(64), nullable=True)
    target: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    refusal_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    approval_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    execution_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    verification_outcome: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.clock_timestamp()
    )
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class HookRun(Base):
    """One claimed trigger slot for an agent version."""

    __tablename__ = "hook_runs"
    __table_args__ = (
        UniqueConstraint(
            "agent_id",
            "name",
            "slot_utc",
            name="hook_runs_agent_name_slot_key",
        ),
        CheckConstraint(
            "outcome IS NULL OR outcome IN "
            "('ran', 'deferred', 'skipped', 'blocked', 'reclaimed', 'failed')",
            name="hook_runs_outcome_ck",
        ),
        CheckConstraint(
            "source IN ('schedule', 'manual')",
            name="hook_runs_source_ck",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE")
    )
    name: Mapped[str] = mapped_column(String)
    slot_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agent_versions.id", ondelete="CASCADE")
    )
    source: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'schedule'")
    )
    outcome: Mapped[str | None] = mapped_column(String, default=None)
    reason: Mapped[str | None] = mapped_column(Text, default=None)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    # When an open claim becomes reclaimable by the hook's next fire (#2931).
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Tenant(Base):
    """A self-host appliance's tenant record (#2906).

    First, no-behavior-change slice: a single row is auto-provisioned by
    migration 0048 at a fixed, well-known id so later migrations can
    reference it without a runtime lookup. ``deployment_id`` is an opaque
    identifier for the physical appliance -- NOT a foreign key to
    ``Deployment``/``deployments``, which is the unrelated dev/prod binding
    of an AgentVersion.
    """

    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    deployment_id: Mapped[str] = mapped_column(String)
    idp_config_ref: Mapped[str | None] = mapped_column(default=None)
    retention_policy_ref: Mapped[str | None] = mapped_column(default=None)
    default_provider_policy_ref: Mapped[str | None] = mapped_column(default=None)
    status: Mapped[str] = mapped_column(String, default="active")
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class Principal(Base):
    """A tenant scoped human or service identity (#2907, ADR 0155 step 2).

    Keyed on the IdP subject within a tenant; ``email`` and ``display_name``
    are attributes and never the identity key. A rebuildable projection of the
    customer IdP, with no callers yet.
    """

    __tablename__ = "principals"
    __table_args__ = (
        CheckConstraint("type IN ('human', 'service')", name="principals_type_ck"),
        CheckConstraint(
            "status IN ('active', 'disabled', 'revoked')",
            name="principals_status_ck",
        ),
        CheckConstraint(
            "authorization_version >= 1",
            name="principals_authorization_version_ck",
        ),
        UniqueConstraint(
            "tenant_id", "idp_subject", name="principals_tenant_idp_subject_key"
        ),
        # Target of principal_teams' tenant-scoped foreign key.
        UniqueConstraint("tenant_id", "id", name="principals_tenant_id_id_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey(f"{SCHEMA}.tenants.id"))
    idp_subject: Mapped[str] = mapped_column(String)
    type: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="active", server_default="active")
    display_name: Mapped[str | None] = mapped_column(default=None)
    email: Mapped[str | None] = mapped_column(default=None)
    last_seen_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    authorization_version: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1"
    )


class Team(Base):
    """A tenant scoped team: an IdP group projection or a Curie-managed team (#2907).

    An ``idp_group`` team carries the IdP's group id in ``external_id``; the
    IdP stays the system of record for its membership.
    """

    __tablename__ = "teams"
    __table_args__ = (
        CheckConstraint(
            "source IN ('idp_group', 'curie_managed')", name="teams_source_ck"
        ),
        CheckConstraint(
            "source <> 'idp_group' OR external_id IS NOT NULL",
            name="teams_idp_group_external_id_ck",
        ),
        UniqueConstraint(
            "tenant_id",
            "source",
            "external_id",
            name="teams_tenant_source_external_id_key",
        ),
        # Target of principal_teams' tenant-scoped foreign key.
        UniqueConstraint("tenant_id", "id", name="teams_tenant_id_id_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey(f"{SCHEMA}.tenants.id"))
    source: Mapped[str] = mapped_column(String)
    external_id: Mapped[str | None] = mapped_column(default=None)
    name: Mapped[str] = mapped_column(String)


class PrincipalTeam(Base):
    """One principal's membership of one team, as last synced (#2907).

    A projection of the IdP's group membership, not a system of record;
    ``version`` and ``synced_at`` record which sync produced the row. Both
    foreign keys include ``tenant_id``, so a principal and a team from
    different tenants cannot be linked.
    """

    __tablename__ = "principal_teams"
    __table_args__ = (
        CheckConstraint(
            "source IN ('idp_group', 'curie_managed')",
            name="principal_teams_source_ck",
        ),
        CheckConstraint("version >= 1", name="principal_teams_version_ck"),
        ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{SCHEMA}.principals.tenant_id", f"{SCHEMA}.principals.id"],
            ondelete="CASCADE",
            name="principal_teams_principal_fkey",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "team_id"],
            [f"{SCHEMA}.teams.tenant_id", f"{SCHEMA}.teams.id"],
            ondelete="CASCADE",
            name="principal_teams_team_fkey",
        ),
        Index("ix_principal_teams_team_id", "team_id"),
    )

    principal_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    team_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    source: Mapped[str] = mapped_column(String)
    version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class FactoryPollCursor(Base):
    """One repository's factory poll cursors and conditional-request tags (#3745)."""

    __tablename__ = "factory_poll_cursors"
    __table_args__ = (
        CheckConstraint(
            "repository_id IS NULL OR repository_id > 0",
            name="factory_poll_cursors_repository_id_ck",
        ),
        CheckConstraint(
            "jsonb_typeof(etags) = 'object'",
            name="factory_poll_cursors_etags_object_ck",
        ),
    )

    repo_full_name: Mapped[str] = mapped_column(Text, primary_key=True)
    repository_id: Mapped[int | None] = mapped_column(BigInteger, default=None)
    comments_since: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    review_comments_since: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    reviews_since: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    etags: Mapped[dict[str, Any]] = mapped_column(JSONB,
        nullable=False, server_default=text("'{}'::jsonb"), default=dict)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


# A secret-store reference, never a value (#2909): ``env:NAME`` or
# ``k8s-secret:name/key``. The DB CHECK and the API's 422 both use it.
PROVIDER_REFERENCE_PATTERN = (
    r"^(env:[A-Z_][A-Z0-9_]*"
    r"|k8s-secret:[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?/[-._a-zA-Z0-9]{1,253})$"
)
PROVIDER_REFERENCE_MAX_LENGTH = 512
# Refuses the shapes of well-known credentials the grammar above would admit
# (a Secret key may hold hyphens, so ``k8s-secret:x/xoxb-...`` would pass):
# a segment opening with a known token prefix, an env name that is an AWS
# access key id, or a 32+ character hex run (signing secrets, hex tokens).
# Best effort against a pasted value; no syntax can prove a string is not one.
PROVIDER_REFERENCE_DENY_PATTERN = (
    r"[:/](xox[a-z]-|xoxe\.|xapp-|gh[pousr]_|github_pat_|sk-|sk_live_|rk_live_|lin_api_|AIza|eyJ)"
    r"|^env:(AKIA|ASIA)[A-Z0-9]{16}$"
    r"|[0-9a-fA-F]{32}"
)


def _provider_reference_check(table: str, column: str) -> CheckConstraint:
    return CheckConstraint(
        f"{column} IS NULL OR (length({column}) <= {PROVIDER_REFERENCE_MAX_LENGTH} "
        f"AND {column} ~ '{PROVIDER_REFERENCE_PATTERN}' "
        f"AND {column} !~ '{PROVIDER_REFERENCE_DENY_PATTERN}')",
        name=f"{table}_{column}_ck",
    )


_PROVIDER_CK = (
    "provider IN ('slack', 'm365', 'github', 'jira', 'linear', "
    "'confluence', 'quickbooks', 'other')"
)


class ProviderInstallation(Base):
    """A connected external account the tenant authorised (#2909, ADR 0166 step 4).

    ADR 0193 decision 3 restores this to ADR 0166's original meaning after ADR
    0168 decision 1 had made it one row per channel identity; that meaning now
    belongs to :class:`ChannelIdentity`. ``authority`` is the provider endpoint
    ``external_account_id`` belongs to: empty for a provider with one global
    service (Slack, Google, Atlassian Cloud), the canonical hostname for a
    self-hosted or per-host one (a GHES instance, an Atlassian Data Center base
    URL) -- so a self-hosted installation's id cannot collide with the same id
    on another host. It is NOT NULL with an empty default, never nullable, so
    two global installations' empty authorities still compare equal for
    uniqueness (Postgres never treats two NULLs as equal). ``disconnected_at``
    is set exactly while the row is disconnected. The installer FK carries
    ``tenant_id``, so the installer must be a principal of the same tenant.
    """

    __tablename__ = "provider_installations"
    __table_args__ = (
        CheckConstraint(_PROVIDER_CK, name="provider_installations_provider_ck"),
        CheckConstraint(
            "status IN ('connected', 'disconnected')",
            name="provider_installations_status_ck",
        ),
        CheckConstraint(
            "(status = 'disconnected') = (disconnected_at IS NOT NULL)",
            name="provider_installations_disconnected_at_ck",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "installed_by_principal_id"],
            [f"{SCHEMA}.principals.tenant_id", f"{SCHEMA}.principals.id"],
            name="provider_installations_installer_fkey",
        ),
        UniqueConstraint(
            "tenant_id",
            "provider",
            "authority",
            "external_account_id",
            name="provider_installations_tenant_provider_authority_account_key",
        ),
        # Lets channel_identities.provider_installation_id's composite FK
        # require an identity to attach only to an installation of its own
        # tenant and provider; `id` alone is already the primary key.
        UniqueConstraint(
            "tenant_id",
            "provider",
            "id",
            name="provider_installations_tenant_provider_id_key",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.tenants.id", name="provider_installations_tenant_id_fkey")
    )
    provider: Mapped[str] = mapped_column(String)
    authority: Mapped[str] = mapped_column(String, default="", server_default="")
    external_account_id: Mapped[str] = mapped_column(String)
    display_name: Mapped[str | None] = mapped_column(default=None)
    status: Mapped[str] = mapped_column(String, default="connected", server_default="connected")
    installed_by_principal_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), default=None
    )
    installed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    disconnected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )


class ChannelIdentity(Base):
    """Who Curie speaks as: one row per bot (#2909, ADR 0168 decision 1, as
    scoped by ADR 0193 decision 4).

    ``name`` is unique within the provider and tenant and is what a binding's
    ``adapter`` names (ADR 0168 decision 3): one Slack workspace with two bots
    is two installations' worth of identities sharing nothing but the tenant
    and provider, never a ``name``. ``attributes`` holds whatever that
    provider's identity needs beyond the fixed columns -- for Slack, the
    app-token reference alongside ``credential_ref``'s bot-token reference.

    ``credential_ref`` and ``webhook_verification_ref`` point into the
    deployment's secret store; the CHECKs hold them to the reference grammar
    and refuse well-known credential shapes, whoever writes them.

    ``provider_installation_id`` is nullable: a declared identity is created at
    boot unattached, since only the identity's own credential can later report
    which installation it belongs to (#3039); until then, an operator attaches
    it through the admin routes. ``installation_mismatch`` is set when a later
    report names a different installation than the identity is attached to; it
    does not detach the identity on its own.
    """

    __tablename__ = "channel_identities"
    __table_args__ = (
        CheckConstraint(_PROVIDER_CK, name="channel_identities_provider_ck"),
        CheckConstraint(
            "status IN ('active', 'disabled', 'revoked')",
            name="channel_identities_status_ck",
        ),
        _provider_reference_check("channel_identities", "credential_ref"),
        _provider_reference_check("channel_identities", "webhook_verification_ref"),
        ForeignKeyConstraint(
            ["tenant_id", "provider", "provider_installation_id"],
            [
                f"{SCHEMA}.provider_installations.tenant_id",
                f"{SCHEMA}.provider_installations.provider",
                f"{SCHEMA}.provider_installations.id",
            ],
            name="channel_identities_installation_fkey",
        ),
        UniqueConstraint(
            "tenant_id",
            "provider",
            "name",
            name="channel_identities_tenant_provider_name_key",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.tenants.id", name="channel_identities_tenant_id_fkey")
    )
    provider: Mapped[str] = mapped_column(String)
    # Unique with (tenant_id, provider); what a binding's `adapter` names
    # (ADR 0168 decision 3). "default" is the one identity an install need not
    # name explicitly.
    name: Mapped[str] = mapped_column(String, default="default", server_default="default")
    credential_ref: Mapped[str | None] = mapped_column(default=None)
    scopes: Mapped[list[str]] = mapped_column(JSONB, default=list, server_default="[]")
    webhook_verification_ref: Mapped[str | None] = mapped_column(default=None)
    # Provider-specific identity details that don't fit a fixed column: for
    # Slack, today just the app-token reference; later, once something
    # resolves them, the team/app/bot user ids.
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default="{}")
    status: Mapped[str] = mapped_column(String, default="active", server_default="active")
    installation_mismatch: Mapped[bool] = mapped_column(default=False, server_default="false")
    provider_installation_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), default=None
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
