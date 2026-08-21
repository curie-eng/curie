"""Deployment-to-runtime binding: resolve a channel address to the agent, its
active deployment, and the bundle + budget to boot the sandbox with.

The B1/J1/L1 Postgres tables are the source of truth. Rather than import the API
package (which would pull FastAPI and its ORM into the worker), this is a thin
read-only query layer over the same tables via a SQLAlchemy async engine: one
parameterized SELECT joining agents -> agent_channels -> deployments ->
agent_versions.

Resolution rule: an agent holds one or more rows in ``agent_channels``
(ADR-0096, #1459; ADR-0116). Resolution is from the ``(kind, address)`` pair to
the agent, so the count of bindings per agent never affects the predicate. The
run uses that agent's active
deployment (deployments.status = 'active'); when both a prod and a dev
deployment are active, prod wins, then the most recent. An address with no
agent, or an agent with no active deployment, resolves to None -- the kernel
answers with a polite placeholder and drops the event rather than crashing.
Per-channel dev/prod bot-identity routing (the dispatcher carrying which bot was
addressed) is a J1/dispatcher refinement noted for later.

Contract (cross-lane, load-bearing): ``agent_channels.address`` MUST store the
channel address the dispatcher enqueues as ``QueuedTurn.reply_handle.channel``,
because the kernel passes that value into ``resolve()`` and this resolver
matches on equality. If the create-agent API/UI stores a Slack channel NAME
(``#triage``) instead of its ID, every real mention resolves to None and is
dropped. Storing the address correctly at agent creation (or translating it
there) is the API/UI's responsibility; this resolver deliberately does not
call any adapter's API to translate, to avoid coupling the worker to a
per-adapter token.

``agent_channels.kind`` ROUTES: since ADR-0096 phase 2 the queue wire
(``ReplyHandle``) carries a required ``kind``, so the routing key is the PAIR
(``kind`` AND ``address``) and migration 0023 widens the uniqueness constraint to
match. There is no address-only overload and no default kind -- either would be
the silent address-fallback the pair exists to remove. One address can now
legitimately be bound twice under two different kinds, and each turn reaches its
own agent.

The binding row also carries the server-controlled reply route: ``endpoint`` (the
channel API base URL this kind's replies go back through) and ``adapter`` (the
egress adapter identity whose credential authenticates them). Both are read here
so the worker gets the route from the same query that resolves the agent, never
from an ingress request body. ``slack`` legitimately carries neither: its route
is the worker's configured Slack origin.

A pair that is not bound resolves to None -- a polite drop naming both halves,
never a fallback to the address alone, because that fallback is exactly the
silent misroute (#38) this predicate closes.
"""

from __future__ import annotations

import json
import logging
import re
import secrets
import time
import uuid
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from aci_protocol import BootEnv, Budget
from plugin_format import is_reserved_boot_env_name
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from . import sandbox_token
from .behaviorpacks import BehaviorPacks
from .config import WorkerConfig

logger = logging.getLogger(__name__)

# Env vars the worker injects into a bound sandbox claim, named from the ONE
# declaration in ``aci_protocol.BootEnv`` (#488, ADR-0049). These are aliases for
# the lanes that cannot go through ``BootEnv.render_worker`` -- the kernel's
# resume overlay, the substrates, and the eval consumer -- never a second
# declaration: retyping the literal here is the drift #488 closes, since a rename
# on one side would leave the sandbox booting fine with the feature silently
# dropped. ``env_key`` raises on an unknown field, so a typo fails at import.
#
# CURIE_BUNDLE_REF is the RustFS object key sandbox provisioning fetches into
# CURIE_PLUGIN_DIR (a runner/chart handoff); the rest are the frozen ACI
# SessionConfig env.
BUNDLE_REF_ENV = BootEnv.env_key("bundle_ref")
PLUGIN_DIR_ENV = BootEnv.env_key("plugin_dir")
BUDGET_ENV = BootEnv.env_key("budget")
SESSION_ID_ENV = BootEnv.env_key("session_id")
FAKE_MODEL_ENV = BootEnv.env_key("fake_model")
CREDENTIALS_ENV = BootEnv.env_key("credentials_ref")
# The memory port (#264): the API key the runner authenticates with when it
# dereferences the agent's memory namespace on the state API at boot. A scoped
# ``state`` token (ADR-0033), and a runner-local knob rather than part of the
# frozen ACI env, like CURIE_RUNNER_TOKEN. The namespace URL itself rides in
# the frozen SessionConfig's memory_ref, which render_worker emits.
MEMORY_TOKEN_ENV = BootEnv.env_key("memory_token")
# The conversation-history port (#20, ADR-0029): the URL of THIS thread's
# transcript key on the same durable state store, dereferenced by the runner at
# boot to rehydrate the conversation after an unplanned restart, plus the API key
# it authenticates with. Both are runner-local knobs, NOT frozen ACI env.
HISTORY_REF_ENV = BootEnv.env_key("history_ref")
HISTORY_TOKEN_ENV = BootEnv.env_key("history_token")
BASE_URL_ENV = BootEnv.env_key("base_url")
# The endpoint's wire protocol (#514), declared so an OpenAI-shaped endpoint
# fails loudly in the runner instead of being silently mis-dialed.
API_BACKEND_ENV = BootEnv.env_key("api_backend")
# Which env var(s) carry the model credential (#514): a bare name or a JSON array.
MODEL_ENV_KEY_ENV = BootEnv.env_key("model_env_key")
MODEL_ENV = BootEnv.env_key("model")
# Thinking depth (#1182, ADR-0098): the model half's sibling, same producer and
# same consumer, so it is named from BootEnv rather than typed as a literal.
THINKING_ENV = BootEnv.env_key("thinking")
# Per-claim bearer token the runner enforces on its ACI POST routes (issue #63).
# Not a model credential, so apply_model_env never sees it; minted fresh per claim.
RUNNER_TOKEN_ENV = BootEnv.env_key("runner_token")
# Per-agent permission gates (#245, ADR-0010): comma-separated tool names whose
# calls the runner intercepts via can_use_tool and pauses awaiting approval.
APPROVAL_REQUIRED_ENV = BootEnv.env_key("approval_required_tools")
# Marks which boot-env keys are per-agent connector secrets (ADR-0009, #429).
# The k8s substrate reads it to strip those plaintext values off the value-only
# SandboxClaim CR (their secretKeyRef delivery is #440); the docker substrate
# forwards them directly. The marker and the keys it names are both kept off the
# k8s claim, so a connector secret is never persisted in etcd.
CONNECTOR_SECRET_KEYS_ENV = BootEnv.env_key("connector_secret_keys")
# #430 one-shot post-approval allowance (ADR-0035): a runner-local knob carrying
# the single approved tool name the runner gate lets through once on a resume boot.
GRANT_TOOL_ENV = BootEnv.env_key("approval_grant_tool")
# #544 Decision A2 turn-end reconciliation marker: an authority-free FACT that
# THIS resume boot is resuming a policy-gate approval. Unlike GRANT_TOOL_ENV it
# confers nothing -- the runner reads it only to decide whether to emit an
# observe-only warning when the approved business action never ran.
RESUMED_KIND_ENV = BootEnv.env_key("approval_resumed_kind")
# ADR-0076 Stone 3 (#889, epic #512): the resolved terminal decision
# ('approved'/'rejected'/'expired') of the approval this resume boot is
# resuming from, so the runner can stamp it on the turn's OTel span and close
# the "did an approval get requested" gap ADR-0038 named open. Also an
# authority-free FACT, like RESUMED_KIND_ENV -- confers nothing.
DECISION_ENV = BootEnv.env_key("approval_decision")
# #517/#669 opt-in false-completion check: a runner-local, authority-free,
# observe-only knob, NOT a declared BootEnv field (unlike the keys above, which
# all go through BootEnv.env_key). runner/src/curie_runner/config.py reads
# this as a direct env lookup rather than through the frozen ACI contract, so
# the literal is spelled out here rather than sourced from BootEnv.env_keys().
# Operator scope, like API_BACKEND_ENV/MODEL_ENV_KEY_ENV: forwarded from
# WorkerConfig.false_completion_check, never per-agent.
FALSE_COMPLETION_CHECK_ENV = "CURIE_FALSE_COMPLETION_CHECK"
# the worker re-mints every turn; this only bounds a leaked-token window (ADR-0033)
SANDBOX_TOKEN_TTL_SECONDS = 24 * 60 * 60

# The permission-gate summary prefix. Duplicated (not imported) from
# runner/src/curie_runner/approval.py::summarize_tool_call /
# APPROVAL_SUMMARY_PREFIX -- the worker must not import the runner package at
# runtime, and a pinning test asserts the two literals agree so divergence fails CI.
_PERMISSION_GATE_SUMMARY_PREFIX = "Tool call awaiting approval: "

# The deterministic resume event id shape emitted by
# apps/api/src/curie_api/resumequeue.py::resume_event_id ("approval-<id>-resolved").
# That suffix is a frozen convention; a pinning test guards format divergence.
_RESUME_EVENT_ID_RE = re.compile(
    r"^approval-([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})-resolved$"
)


def _parse_resume_event_id(event_id: str) -> uuid.UUID | None:
    """The approval id embedded in a resume event id, or None if it is not one.

    Returns None (never raises) for a non-approval event id or a malformed uuid,
    so a non-resume turn fast-returns without any DB round-trip.
    """
    match = _RESUME_EVENT_ID_RE.match(event_id)
    if match is None:
        return None
    try:
        return uuid.UUID(match.group(1))
    except ValueError:
        return None

_RESOLVE_SQL = """
SELECT a.id AS agent_id,
       a.name AS agent_name,
       a.max_usd_per_day AS max_usd_per_day,
       a.max_output_tokens_per_run AS max_output_tokens_per_run,
       a.behavior_packs AS behavior_packs,
       a.model AS model,
       a.thinking AS thinking,
       a.approval_required_tools AS approval_required_tools,
       a.approval_routes AS approval_routes,
       a.secrets AS secrets,
       v.id AS version_id,
       v.version_label AS version_label,
       v.bundle_ref AS bundle_ref,
       c.endpoint AS endpoint,
       c.adapter AS adapter
FROM {schema}.agents a
JOIN {schema}.agent_channels c ON c.agent_id = a.id
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
WHERE c.kind = :kind AND c.address = :address
ORDER BY (d.environment = 'prod') DESC, d.deployed_at DESC
"""


class ResolvedDeployment(BaseModel):
    """The agent binding for a channel: which version to run and its budget."""

    agent_id: uuid.UUID
    # The agent's NAME, not just its id: connector object names are agent-scoped
    # (#1116) and use the name, so the runner needs it to derive a URL that
    # matches the Service that exists.
    agent_name: str
    version_id: uuid.UUID
    version_label: str
    bundle_ref: str | None
    max_usd_per_day: float | None
    max_output_tokens_per_run: int | None
    # The agent's opt-in behavior packs (declarative JSON), or None for the
    # all-off platform default. Parsed into a BehaviorPacks via packs_for().
    behavior_packs: dict[str, Any] | None = None
    # The agent's pinned model id (#254), forwarded as CURIE_MODEL at boot.
    # None falls back to the worker's configured default model.
    model: str | None = None
    # The agent's thinking depth (#1182, ADR-0098), forwarded as CURIE_THINKING
    # at boot. None falls back to the worker's configured default; unset at both
    # layers sends nothing and leaves the model's own default standing.
    thinking: str | None = None
    # The agent's permission gates (#245): tool names requiring human approval,
    # forwarded as CURIE_APPROVAL_REQUIRED_TOOLS at boot. None means no gates.
    approval_required_tools: list[str] | None = None
    # The agent's approval route bindings (#247): manifest route name ->
    # workspace binding ({"channel": "C..."}), resolved by the kernel when a
    # raised approval names a route. None means no bindings.
    approval_routes: dict[str, Any] | None = None
    # The agent's connector secrets (ADR-0009, #429): env-var name -> secret
    # value, injected by name into the sandbox boot env so a bundle's authed MCP
    # server can read its token via `.mcp.json` `${VAR}` expansion. None means no
    # connector secrets. (Local tier stores values on the agent row; the cluster
    # tier delivers them via a per-agent K8s Secret instead.)
    secrets: dict[str, str] | None = None
    # The binding row's server-controlled reply route (ADR-0096 phase 2). The
    # channel API base URL this kind's replies go back through, and the egress
    # adapter identity whose credential authenticates them. Both NULL for
    # `slack`, whose route is the worker's configured Slack origin; both set
    # together for any other kind (`agent_channels_route_pair_ck`).
    endpoint: str | None = None
    adapter: str | None = None


def warn_if_multiple_agents_bound(kind: str, address: str, rows: Sequence[Any]) -> None:
    """Warn when a binding resolves to more than one agent, naming the shadowed.

    Takes the routing PAIR, not a pre-joined string: the pair is what the
    resolver looked up, and an address means nothing without the kind it was
    bound under (ADR-0096 -- the same address can legitimately exist under two
    kinds).

    The ORDER BY picks one deterministic winner (prod-first, then most recent).
    The API enforces one agent per bound pair (``agent_channels_kind_address_key``,
    migration 0023, superseding 0021's address-only ``agent_channels_address_key``
    and 0017's ``agents_slack_channel_key``), so this state is no longer
    reachable through the write paths. It stays as defense in depth for
    rows predating the constraint or written out of band, and because silently
    shadowing an agent is the failure mode #38 existed to kill.

    One agent with both a dev and a prod deployment active is two rows but one
    agent, so count distinct agents, not rows.

    ``rows`` is deliberately typed structurally rather than as SQLAlchemy's
    ``RowMapping``: the helper only subscripts ``agent_id``, and naming the driver
    type here would re-couple a function whose whole point is to be DB-free.

    Pure and DB-free on purpose (#959): the branch is unreachable while the
    constraint holds, so the only honest way to cover it was previously to DROP
    that constraint against a shared developer database and restore it in a
    `finally` -- which left the production invariant absent if the process died
    in between. Taking rows as an argument moves the coverage to a unit test and
    removes the destructive DDL entirely.
    """

    distinct_agents = {r["agent_id"] for r in rows}
    if len(distinct_agents) <= 1:
        return
    chosen = rows[0]["agent_id"]
    shadowed = sorted(str(a) for a in distinct_agents if a != chosen)
    logger.warning(
        "channel %s has %d agents bound; routing to agent %s and shadowing "
        "%s (only one agent per channel responds; see issue #38)",
        f"{kind}:{address}",
        len(distinct_agents),
        chosen,
        ", ".join(shadowed),
    )


class BindingResolver:
    """Resolves a channel address to its active agent deployment (read-only)."""

    def __init__(self, engine: AsyncEngine, config: WorkerConfig) -> None:
        self._engine = engine
        self._config = config
        # Table identifiers are not user input; the schema comes from config.
        self._sql = text(_RESOLVE_SQL.format(schema=config.db_schema))

    async def resolve(self, kind: str, address: str) -> ResolvedDeployment | None:
        """Resolve the ``(kind, address)`` routing pair to its active deployment.

        Both halves are required and neither has a default: an address-only
        overload would silently answer an unbound kind with somebody else's
        agent, which is #38's misroute wearing the neutral-binding hat.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(self._sql, {"kind": kind, "address": address})
            rows = result.mappings().all()
        if not rows:
            return None
        warn_if_multiple_agents_bound(kind, address, rows)
        data = dict(rows[0])
        # asyncpg returns JSONB as a str for a raw-text SELECT (no column type to
        # trigger SQLAlchemy's json deserializer); decode it to the dict/list the
        # model expects. A dict/list (or None) passes through untouched.
        packs = data.get("behavior_packs")
        if isinstance(packs, str):
            data["behavior_packs"] = json.loads(packs)
        gates = data.get("approval_required_tools")
        if isinstance(gates, str):
            data["approval_required_tools"] = json.loads(gates)
        routes = data.get("approval_routes")
        if isinstance(routes, str):
            data["approval_routes"] = json.loads(routes)
        conn_secrets = data.get("secrets")
        if isinstance(conn_secrets, str):
            data["secrets"] = json.loads(conn_secrets)
        return ResolvedDeployment.model_validate(data)

    async def repo_full_name(self, agent_id: uuid.UUID) -> str | None:
        """The agent's GitHub repo (owner/name), for the eval PR-check report."""
        sql = text(f"SELECT repo_full_name FROM {self._config.db_schema}.agents WHERE id = :id")
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id})
            row = result.first()
        if row is None:
            return None
        value: str | None = row[0]
        return value

    async def approval_grant_tool(
        self, event_id: str, agent_id: uuid.UUID
    ) -> str | None:
        """The one-shot post-approval grant for a resume turn (#430, ADR-0035).

        When ``event_id`` is the deterministic resume id of a genuinely
        ``approved`` PERMISSION-GATE approval, return the single approved tool
        name the runner gate should let through once; otherwise None. Derived
        server-side from the durable ``approvals`` row, so a compromised sandbox
        cannot mint one.

        Provenance is a COLUMN, not a string prefix (#544, Decision C). The
        runner writes ``gate_kind`` and ``granted_tool``; this method returns the
        ``granted_tool`` column and does not re-derive it. Three cases on
        ``gate_kind``:

        * ``'policy'`` or ``'permission'`` -- return the ``granted_tool`` column
          (or None when NULL). For a permission gate this is the trusted
          ``can_use_tool`` value the runner denied. For a policy gate, #558
          (superseding ADR-0046 Decision C's outright refusal) lets an
          operator-opted ``grantableViaPolicy`` gate carry a grant: the runner
          stamps the MANIFEST-declared tool onto ``granted_tool`` for exactly
          those gates and leaves it NULL for every other policy gate, so honoring
          the column grants the opted-in gates and preserves #544's no-grant
          default (NULL -> None) elsewhere. The value is never model-authored
          (runner-sourced from the manifest), never a parse of the summary, so
          the #430/#410 forgery seam stays closed.
        * ``NULL`` -- the rolling-deploy window (edge case 7): a NEW worker met an
          OLD pinned runner whose final carried no provenance. Fall back to the
          legacy summary-prefix parse -- byte-identical to today's behavior, so a
          no-op for old rows that cannot widen anything. Deleted once no old
          runner can be live (follow-up 2).

        The grant is agent-bound: the row's ``agent_id`` MUST be non-NULL and
        equal ``agent_id`` (the agent currently resolved for this channel).
        A NULL row agent_id or a mismatch returns None -- fail-safe, never a
        cross-agent grant. This closes a rebind leak: if a channel is rebound to
        a different agent while an approval is pending, agent A's grant must not
        be injected into agent B's runner and cross-authorize a shared tool name.

        A non-approval event id fast-returns None with no DB round-trip.
        """
        approval_id = _parse_resume_event_id(event_id)
        if approval_id is None:
            return None
        sql = text(
            f"SELECT status, summary, agent_id, gate_kind, granted_tool "
            f"FROM {self._config.db_schema}.approvals WHERE id = :id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": approval_id})
            row = result.mappings().first()
        if row is None:
            return None
        # Literal status compare: the worker must not import the API's ApprovalStatus.
        if row["status"] != "approved":
            return None
        # Agent-bind the grant: never cross-authorize across a channel rebind.
        # Evaluated BEFORE the provenance branch so a mismatch is refused by the
        # agent-bind guard regardless of gate_kind (the load-bearing #430 order).
        row_agent_id = row["agent_id"]
        if row_agent_id is None or row_agent_id != agent_id:
            return None
        gate_kind = row["gate_kind"]
        if gate_kind in ("policy", "permission"):
            # #558 (supersedes ADR-0046 Decision C's "policy refuses outright"): a
            # policy gate mints a grant ONLY when the operator opted its manifest
            # gate into grantability (grantableViaPolicy). The runner stamps the
            # manifest-declared tool onto granted_tool for exactly those gates and
            # leaves it NULL otherwise, so honoring the column grants the opted-in
            # gates and preserves #544's no-grant default (NULL -> None) for every
            # other policy gate. granted_tool is never model-authored (the runner
            # sources it from the manifest), so this cannot be reached by a
            # prompt-injected model. The permission path is unchanged.
            tool: str | None = row["granted_tool"]
            return tool or None
        # gate_kind IS NULL: the old-runner fallback, today's prefix parse.
        summary: str | None = row["summary"]
        if not summary or not summary.startswith(_PERMISSION_GATE_SUMMARY_PREFIX):
            return None
        tool = summary[len(_PERMISSION_GATE_SUMMARY_PREFIX):].split(" ", 1)[0]
        return tool or None

    async def approval_resumed_kind(
        self, event_id: str, agent_id: uuid.UUID
    ) -> str | None:
        """The gate provenance of the approval a resume turn is resuming (#544,
        Decision A2), or None.

        An authority-free FACT about the past for the runner's OBSERVE-ONLY
        turn-end reconciliation -- unlike ``approval_grant_tool`` it confers
        nothing, it only tells the runner "this boot is resuming a policy-gate
        approval" so it can warn if the approved business action never ran. The
        marker granting nothing is exactly why #430 and #410 stay closed.

        Agent-bound identically to the grant so it never leaks across a channel
        rebind, and NULL for a non-approval event, a non-``approved`` status
        (rejected/expired/pending resume the same event id shape, but no approved
        action was owed so no marker is due), an unknown or other-agent approval,
        or an old-runner row whose provenance column is NULL.
        """
        approval_id = _parse_resume_event_id(event_id)
        if approval_id is None:
            return None
        sql = text(
            f"SELECT status, agent_id, gate_kind "
            f"FROM {self._config.db_schema}.approvals WHERE id = :id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": approval_id})
            row = result.mappings().first()
        if row is None:
            return None
        # Literal status compare: the worker must not import the API's
        # ApprovalStatus. A rejected/expired/pending resume did nothing that was
        # owed, so it must not inject a marker that provokes a false
        # approval-not-acted warning.
        if row["status"] != "approved":
            return None
        # Agent-bind guard stays after the status gate, mirroring the grant.
        row_agent_id = row["agent_id"]
        if row_agent_id is None or row_agent_id != agent_id:
            return None
        kind: str | None = row["gate_kind"]
        return kind or None

    async def approval_decision(self, event_id: str, agent_id: uuid.UUID) -> str | None:
        """The resolved terminal decision of the approval a resume turn is
        resuming from (ADR-0076 Stone 3, #889), or None.

        An authority-free FACT for the runner's OTel span, exactly like
        ``approval_resumed_kind`` -- it confers nothing, it only reports an
        outcome the worker already resolved. Unlike ``approval_resumed_kind``
        (approved-only, since only an approved resume owes a business action)
        this reports all three terminal statuses -- ``approved``, ``rejected``,
        and ``expired`` -- so a rejected or expired gate is observable from the
        trace too, closing the "did an approval get requested" gap ADR-0038
        named open. ``pending`` is not terminal and is never returned.

        Agent-bound identically to the grant and the resumed-kind marker, so it
        never leaks across a channel rebind. A non-approval event id, an
        unknown or other-agent approval, or a still-pending approval all
        return None.
        """
        approval_id = _parse_resume_event_id(event_id)
        if approval_id is None:
            return None
        sql = text(
            f"SELECT status, agent_id FROM {self._config.db_schema}.approvals WHERE id = :id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": approval_id})
            row = result.mappings().first()
        if row is None:
            return None
        row_agent_id = row["agent_id"]
        if row_agent_id is None or row_agent_id != agent_id:
            return None
        # Literal status compare: the worker must not import the API's
        # ApprovalStatus.
        status: str = row["status"]
        if status not in ("approved", "rejected", "expired"):
            return None
        return status

    async def secrets_for(self, agent_id: uuid.UUID) -> dict[str, str] | None:
        """The agent's connector secrets (#429), for lanes that boot by agent_id
        rather than by channel (the eval consumer). Decodes the JSONB the same
        way ``resolve`` does; None when the agent is unknown or has no secrets."""
        sql = text(f"SELECT secrets FROM {self._config.db_schema}.agents WHERE id = :id")
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id})
            row = result.first()
        if row is None or row[0] is None:
            return None
        value = row[0]
        if isinstance(value, str):
            value = json.loads(value)
        return value if isinstance(value, dict) else None

    async def thinking_for(self, agent_id: uuid.UUID) -> str | None:
        """The agent's thinking depth for eval sandbox boots."""
        sql = text(f"SELECT thinking FROM {self._config.db_schema}.agents WHERE id = :id")
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id})
            row = result.first()
        if row is None:
            return None
        value: str | None = row[0]
        return value

    def packs_for(self, resolved: ResolvedDeployment) -> BehaviorPacks:
        """The agent's parsed behavior packs (all-off when none are configured).

        The kernel wiring that samples a working line / short-circuits a greeting
        consumes this; it is a separate, F1-reviewed change (docs/behavior-packs.md).
        """
        return BehaviorPacks.from_config(resolved.behavior_packs)

    def budget_for(self, resolved: ResolvedDeployment) -> Budget:
        """The CURIE_BUDGET for the agent, applying platform defaults for NULLs."""
        return Budget(
            max_output_tokens_per_run=(
                resolved.max_output_tokens_per_run
                if resolved.max_output_tokens_per_run is not None
                else self._config.default_max_output_tokens_per_run
            ),
            max_usd_per_day=(
                resolved.max_usd_per_day
                if resolved.max_usd_per_day is not None
                else self._config.default_max_usd_per_day
            ),
        )

    def boot_env(self, resolved: ResolvedDeployment, thread_key: str) -> dict[str, str]:
        """The env injected into the sandbox claim for a bound run.

        Rendered from the declared contract (``BootEnv``, #488/ADR-0049) rather
        than a hand-built dict, so every name here is typed once, in one place,
        and a rename cannot leave the sandbox booting fine with a silently
        dropped feature. ``render_worker`` emits only the worker-authoritative
        keys: it never writes CURIE_SANDBOX_ID or CURIE_RUNNER_PORT, which
        the substrate derives from the pod itself.
        """
        # The memory ref (#264): the agent's scoped namespace on the durable
        # state store (#23/#248). The runner dereferences it at boot to load
        # prior memory and to append learned records with provenance.
        #
        # Minted from the RUNNER-facing API base, not the worker's self-dial
        # api_base_url (#678): the runner dereferences these refs, and in the
        # docker substrate it lives on the bridge runner network where the
        # worker's host-net localhost URL is unreachable, so a self-dial base
        # left every spawn "booting without memory/history". runner_facing_api_
        # base_url falls back to api_base_url (byte-identical to before) when the
        # runner-facing base is not split out.
        base = self._config.runner_facing_api_base_url.rstrip("/")
        memory_ref = f"{base}/agents/{resolved.agent_id}/state/memory"
        # The history ref (#20, ADR-0029): this thread's transcript key on the
        # same state store. It is deterministic per (agent, thread), so a fresh,
        # restarted, or resumed sandbox all boot with the same ref and the runner
        # rehydrates the conversation identically -- an unplanned restart needs no
        # special branch. thread_key is URL-encoded so a channel/ts with reserved
        # characters cannot break the key path.
        thread_segment = quote(thread_key, safe="")
        history_ref = f"{base}/agents/{resolved.agent_id}/state/transcript/{thread_segment}"
        # The general state namespace base (#249): the agent's whole state
        # subtree on the same store. The auto-mounted ``curie-state`` MCP server
        # and any bundle script talking to the store directly compose
        # ``/<namespace>/<key>`` onto this. Memory and history are two reserved
        # namespaces UNDER it; a bundle skill gets the rest.
        state_url = f"{base}/agents/{resolved.agent_id}/state"
        # Mint scoped tokens (ADR-0033, #410) for this agent. Two scopes, because
        # the memory/history loaders and the bundle reach DIFFERENT namespaces:
        #  - the broad ``state`` token backs the memory and history tokens, whose
        #    loaders MUST read/write the reserved ``memory``/``transcript``
        #    namespaces to rehydrate the agent across suspend/resume;
        #  - the narrow ``state.app`` token backs the bundle-facing state token,
        #    which the API state router refuses on those reserved namespaces
        #    (#249) -- so a skill cannot corrupt memory/history by composing the
        #    mounted ``CURIE_STATE_URL`` directly with the token it holds.
        # The scope strings are mirrored in ``apps/api`` ``routers/state.py``
        # (STATE_SCOPE / STATE_APP_SCOPE). When no platform key is configured
        # (fake/local) there is nothing to sign with, so neither token is minted
        # and none is set -- preserving the pre-#410 no-key path.
        state_token: str | None = None
        app_state_token: str | None = None
        if self._config.api_key:
            exp = int(time.time()) + SANDBOX_TOKEN_TTL_SECONDS
            state_token = sandbox_token.mint(
                self._config.api_key,
                agent=str(resolved.agent_id),
                scope="state",
                exp=exp,
            )
            app_state_token = sandbox_token.mint(
                self._config.api_key,
                agent=str(resolved.agent_id),
                scope="state.app",
                exp=exp,
            )
        env = BootEnv.render_worker(
            plugin_dir=self._config.bundle_plugin_dir,
            session_id=f"agent-{resolved.agent_id}-thread-{thread_key}",
            budget=self.budget_for(resolved),
            memory_ref=memory_ref,
            history_ref=history_ref,
            bundle_ref=resolved.bundle_ref,
            # Not a model credential, so the model keys never see it; minted
            # fresh per claim and enforced by the runner on its ACI POST routes.
            runner_token=secrets.token_urlsafe(32),
            # The agent's permission gates (#245): the runner intercepts these
            # tool calls via can_use_tool and pauses awaiting approval. Names are
            # comma-joined by the render (validated comma-free at the API).
            approval_required_tools=resolved.approval_required_tools,
            # Where this sandbox's hosted connectors live (ADR-0086, #1118).
            # render_worker emits these as a SET or not at all, so an install
            # missing either operator value yields no scope rather than a
            # partial one naming a Service that cannot exist.
            connector_release=self._config.connector_release or None,
            connector_agent=resolved.agent_name,
            connector_namespace=self._config.connector_namespace or None,
            # The agent's pinned model (#254) overrides the worker default; None
            # falls back to the platform default.
            model=resolved.model if resolved.model is not None else self._config.model,
            # Same precedence as the model above (#1182): the agent's value wins,
            # then the platform default, then nothing at all -- and "nothing at
            # all" is what makes an unconfigured install behave as it always has.
            thinking=(
                resolved.thinking if resolved.thinking is not None else self._config.thinking
            )
            or None,
            fake_model=self._config.fake_model,
            credentials_ref=self._config.credentials,
            base_url=self._config.model_base_url,
            # The endpoint's declared wire protocol and credential key(s) (#514).
            # Operator scope only: read from WorkerConfig, never from the agent
            # row, so no per-agent value can redeclare the wire or aim the
            # credential read. Empty config means undeclared, so the render omits
            # the key and the runner keeps its defaults.
            api_backend=self._config.model_api_backend or None,
            model_env_key=self._config.model_env_key or None,
            history_token=state_token,
            memory_token=state_token,
            # The general state store exposed to bundle code (#249): the NARROW
            # ``state.app`` token authorizes the URL -- refused on the reserved
            # memory/transcript namespaces server-side -- so the token is omitted
            # (and the URL still emitted) on the no-key fake/local path.
            state_url=state_url,
            state_token=app_state_token,
        )
        # #517/#669 opt-in false-completion check: NOT a BootEnv.render_worker
        # kwarg (it is deliberately kept out of the frozen ACI contract, see
        # FALSE_COMPLETION_CHECK_ENV above), so it is written directly here
        # rather than threaded through the render call above. Operator scope
        # only, mirrored in apply_model_env for the eval lane so both boot
        # paths agree.
        if self._config.false_completion_check:
            env[FALSE_COMPLETION_CHECK_ENV] = "1"
        # Deliver the agent's connector secrets (ADR-0009, #429): named secret
        # values the bundle's authed MCP servers read from the sandbox env, where
        # `.mcp.json` `${VAR}` expansion consumes them. Injected by value; the
        # docker substrate forwards them as `-e KEY=VALUE`, while the k8s
        # substrate strips them off its plaintext claim CR by the marker
        # CURIE_CONNECTOR_SECRET_KEYS (their secretKeyRef delivery is #440).
        # Runs AFTER the render so the reserved-name filter sees the rendered
        # keys, and stays the marker's sole writer -- see the
        # inject_connector_secrets docstring for the #457/#429 rationale.
        inject_connector_secrets(
            env, resolved.secrets, agent_label=resolved.agent_id
        )
        return env


def apply_model_env(
    env: dict[str, str],
    config: WorkerConfig,
    model_override: str | None = None,
    thinking_override: str | None = None,
) -> None:
    """Layer the runner model + credentials passthrough onto a boot env.

    Shared by the runs binding and the eval consumer so both lanes boot the
    runner the same way: fake_model gates the canned model (no credential
    needed); credentials is forwarded only when set and never logged. The local
    model demo path injects a generic Anthropic compatible base URL when
    configured; an explicit model is forwarded whenever set.

    ``model_override`` is the per-agent CURIE_MODEL (#254): when set it wins
    over the worker's configured default model, so a single agent can be pinned
    to a specific model. None means "use the platform default" (config.model).
    ``thinking_override`` is its sibling (#1182, ADR-0098) with the same
    precedence and the same ownership: both layers are operator-set, and a
    bundle has no surface for either. Unset at both layers writes nothing, so
    the runner sends no thinking configuration and the model's own default
    stands.

    Those two are the only per-agent knobs here. The api_backend and env_key declarations
    (#514) come from WorkerConfig only and take no override: they select which
    wire protocol is dialed and which env var a credential is read from, so a
    lower-privileged agent author must not be able to set them.

    Also layers the false-completion check (#517, #669): another operator-only,
    no-override knob, forwarded here so the eval lane's boot env agrees with
    ``BindingResolver.boot_env``'s direct write of the same key.
    """
    if config.fake_model:
        env[FAKE_MODEL_ENV] = "1"
    if config.credentials:
        env[CREDENTIALS_ENV] = config.credentials
    if config.model_base_url:
        env[BASE_URL_ENV] = config.model_base_url
    if config.model_api_backend:
        env[API_BACKEND_ENV] = config.model_api_backend
    if config.model_env_key:
        env[MODEL_ENV_KEY_ENV] = config.model_env_key
    model = model_override if model_override is not None else config.model
    if model:
        env[MODEL_ENV] = model
    thinking = thinking_override if thinking_override is not None else config.thinking
    if thinking:
        env[THINKING_ENV] = thinking
    if config.false_completion_check:
        env[FALSE_COMPLETION_CHECK_ENV] = "1"


def inject_connector_secrets(
    env: dict[str, str],
    secrets: dict[str, str] | None,
    *,
    agent_label: object,
) -> None:
    """Inject per-agent connector secrets, dropping reserved boot-env names
    (order-independent, #457). Sets the CURIE_CONNECTOR_SECRET_KEYS marker
    for the keys actually injected. Shared by the runs binding and the eval
    consumer so both write sites stay hardened identically.

    Every connector secret is filtered against the shared reserved-name policy
    (``is_reserved_boot_env_name``) regardless of env ordering, so a secret named
    after an ACI contract env key or a model credential (e.g. ``ANTHROPIC_BASE_URL``)
    can never clobber it -- even on the default path where ``apply_model_env`` does
    not itself set the base URL after the caller runs this. Drop-and-log rather than
    raise (raising would crash a live claim); a dropped key never carries its value
    and is kept out of the marker. The warning carries no connector or agent
    information.
    """
    injected_secret_keys: list[str] = []
    for name, value in (secrets or {}).items():
        if is_reserved_boot_env_name(name):
            logger.warning(
                "Dropping connector secret with reserved boot-env name "
                "(never injected, never marked)"
            )
            continue
        env[name] = value
        injected_secret_keys.append(name)
    if injected_secret_keys:
        env[CONNECTOR_SECRET_KEYS_ENV] = ",".join(sorted(injected_secret_keys))
