"""SQLAlchemy models: agents, agent_versions, deployments.

Kept deliberately minimal (see docs/build-orchestration-plan.md). B2 added the
bundle columns; J1 added the git-flow columns (agents.repo_full_name,
agent_versions.commit_sha, deployments.bot_identity/commit_sha).
"""

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Enum, ForeignKey, Index, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import SCHEMA, Base

GIT_FLOW_CREATED_BY = "git-flow"


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


class Agent(Base):
    __tablename__ = "agents"

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
    # Per-agent thinking depth (#1182, ADR-0098). Forwarded as CURIE_THINKING at
    # sandbox boot; NULL means the worker's CURIE_THINKING default applies, and
    # unset at both layers means the runner sends no thinking configuration and
    # the model's own default stands. Operator-owned like `model` above: a bundle
    # has no surface for it at any tier. The vocabulary is the runner's
    # (`curie_runner.thinking`), not this column's -- stored as a plain string so
    # the persistence layer does not have to track the harness.
    thinking: Mapped[str | None] = mapped_column(default=None)
    # Per-agent behavior packs: declarative, opt-in UX touches the worker applies
    # around a turn (a sampled "working..." line, a canned greeting reply). Stored
    # as JSON here and resolved onto the deployment by the worker's binding layer;
    # NULL means no packs (the platform default). The shape is validated by
    # schemas.BehaviorPacksConfig on write and parsed by
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
    # workspace specifics, today a Slack channel: {"managers": {"channel":
    # "C0123..."}}. The worker resolves a raised route through this map to
    # decide where the approval card goes (and therefore who the
    # channel-membership authorizer counts as approvers). NULL means no
    # bindings; an unbound route falls back to the requesting channel.
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
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    versions: Mapped[list["AgentVersion"]] = relationship(
        back_populates="agent", cascade="all, delete-orphan"
    )
    # The agent's channel bindings (ADR-0096, #1459; PLURAL since ADR-0116,
    # migration 0028): an agent may hold more than one, so the API surface is a
    # list rather than an object.
    #
    # `order_by` is load-bearing, not cosmetic: `agent_channels` has no
    # `created_at` to fall back on, so without an explicit order the serialized
    # list's element order is whatever Postgres happens to return, and two
    # identical GETs could differ. `(kind, address)` is used because it is the
    # pair every other layer already treats as the binding's identity
    # (`binding._RESOLVE_SQL`, `agent_channels_kind_address_key`).
    #
    # `lazy="selectin"` is load-bearing, not a preference: every read path builds
    # `AgentOut` from this attribute after its session has been handed back, and
    # the default lazy strategy RAISES on attribute access outside an await under
    # asyncio instead of loading. Dropping it turns all three read endpoints into
    # 500s while the crud-level tests, which hold a live session, stay green.
    channels: Mapped[list["AgentChannel"]] = relationship(
        back_populates="agent",
        cascade="all, delete-orphan",
        order_by="(AgentChannel.kind, AgentChannel.address)",
        lazy="selectin",
    )


class AgentChannel(Base):
    """Where one agent listens: a channel KIND and an ADDRESS (ADR-0096, #1459).

    Replaces `agents.slack_channel` (migration 0021) so an agent can bind a
    channel kind the platform has never heard of without a schema change.

    `kind` names the owning adapter, selects the address-shape validator
    (`schemas._validate_channel_binding`), AND routes: since ADR-0096 phase 2 the
    queue wire carries a required `ReplyHandle.kind`, so the worker resolves on
    the PAIR and the uniqueness below widened to match (migration 0023). The
    widening was only safe once no address-only consumer could run -- a
    pair-unique constraint under an address-only lookup would let two agents hold
    one address while the resolver could not tell them apart, which is #38's
    silent misrouting wearing a different hat. That ordering is why 0023 lands
    after the cutover proves no old worker is running.

    `endpoint`/`adapter` are the server-controlled reply route: where this kind's
    replies go back through, and which egress credential authenticates them. They
    are set here by the platform and never accepted from an ingress request body.
    `generation` counts rebinds: `update_channel_binding` mutates this row IN PLACE,
    so the row id is a stable identity and the generation is the only thing that
    makes a rebind observable to a credential minted before it.
    """

    __tablename__ = "agent_channels"
    __table_args__ = (
        # One agent per ROUTE, the `(kind, address)` pair (#38, widened from
        # migration 0021's address-only `agent_channels_address_key` by 0023).
        # The worker resolves a pair to an agent, so a second agent bound to the
        # same pair could never respond -- it would be silently shadowed.
        # Enforced here so it fails at create time.
        #
        # The pair, not the address alone, ONLY because the resolver now sees the
        # pair too (`binding._RESOLVE_SQL`). Widening this while any address-only
        # consumer can still run re-opens the exact ambiguity the constraint
        # exists to close, which is why the cutover proves no old worker pod is
        # running before migration 0023 applies.
        UniqueConstraint("kind", "address", name="agent_channels_kind_address_key"),
        # No agent_id uniqueness here (ADR-0116, migration 0028): an agent may
        # hold more than one binding now. ADR-0089's "one agent still binds one
        # channel" is amended in part -- the (kind, address) constraint above is
        # still what stops two agents claiming the same channel; nothing stops
        # one agent from claiming several.
        #
        # PLAIN index on agent_id, because dropping that uniqueness dropped the
        # column's only index with it (migration 0028 recreates it as this).
        # `crud.lock_agent_bindings` filters and orders by agent_id under
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
    # The server-controlled reply route (migration 0024). Both NULL for `slack`,
    # whose route is the worker's configured Slack origin; both set together for
    # any other kind -- `agent_channels_route_pair_ck` states that invariant at
    # the database so a half-configured route cannot be written out of band.
    endpoint: Mapped[str | None] = mapped_column(default=None)
    adapter: Mapped[str | None] = mapped_column(default=None)
    # Rebind counter (ADR-0096 D5). Bumped on every binding write, including one
    # that changes nothing: re-asserting a binding is the "something is wrong
    # with this route" gesture that should invalidate outstanding credentials.
    generation: Mapped[int] = mapped_column(server_default="0", default=0)

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
    status: Mapped[str] = mapped_column(server_default="active")
    deployed_at: Mapped[datetime] = mapped_column(server_default=func.now())


class Approval(Base):
    """A durable human-approval request (#244, ADR-0010).

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

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Nullable: a run without a deployment binding (the generic/dev path) can
    # still gate on a human decision.
    agent_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        index=True,
        default=None,
    )
    # The thread key routing keeps one live session per (the worker's
    # conversation_id); the resume turn is enqueued back onto it.
    conversation_id: Mapped[str] = mapped_column(index=True)
    # Who authored the turn that raised the request. #246 blocks self-approval
    # against this field; recorded now so existing rows carry it.
    author: Mapped[str]
    # The human-readable statement of what needs approval, from the run's
    # approval request (the ACI final's approval_summary).
    summary: Mapped[str]
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


class ApprovalAuditEntry(Base):
    """The platform audit log for approvals (#247, ADR-0010).

    One row per authorization-relevant event on an approval: a resolution that
    won, a denied attempt, an expiry. Each row snapshots WHO acted, from where,
    and the authorizer verdict that counted (or refused) them -- the answer to
    "who resolved, and why they counted" that a black-box approval cannot give.
    Append-only: rows are written by the resolve endpoint and never updated.
    """

    __tablename__ = "approval_audit_entries"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    approval_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.approvals.id", ondelete="CASCADE"), index=True
    )
    # What happened: resolved / denied / race_lost / expired.
    action: Mapped[str]
    actor: Mapped[str]
    actor_channel: Mapped[str | None] = mapped_column(default=None)
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
        UniqueConstraint("agent_id", "namespace", "key", name="uq_state_agent_ns_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    agent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE")
    )
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

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    # SHA-256 hex of the single-use login code. Unique so a hash collision or a
    # duplicate mint cannot produce two rows one code could satisfy.
    login_code_hash: Mapped[str] = mapped_column(unique=True, index=True)
    login_code_expires_at: Mapped[datetime]
    # Set at exchange, so NULL means "minted, never redeemed".
    session_token_hash: Mapped[str | None] = mapped_column(
        default=None, unique=True, index=True
    )
    session_expires_at: Mapped[datetime | None] = mapped_column(default=None)
    # Stamped at exchange; its presence is what makes the code single-use.
    consumed_at: Mapped[datetime | None] = mapped_column(default=None)
    revoked_at: Mapped[datetime | None] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
