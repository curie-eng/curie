"""Deployment-to-runtime binding: resolve a channel address to the agent, its
active deployment, and the bundle + budget to boot the sandbox with.

The B1/J1/L1 Postgres tables are the source of truth. Rather than import the API
package (which would pull FastAPI and its ORM into the worker), this is a thin
read-only query layer over the same tables via a SQLAlchemy async engine: one
parameterized SELECT joining agents -> agent_channels -> deployments ->
agent_versions.

Resolution rule: an agent holds one or more rows in ``agent_channels``
(ADR-0096, #1459; ADR-0118). Resolution is from the ``(kind, address)`` pair to
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
match; migration 0070 widens it again to the route triple, so several
identities may share a pair. There is no address-only overload and no default
kind -- either would be the silent address-fallback the pair exists to remove.
One address can now legitimately be bound twice under two different kinds, and
each turn reaches its own agent.

The binding row also carries the server-controlled reply route: ``endpoint`` (the
channel API base URL this kind's replies go back through) and ``adapter`` (the
egress adapter identity whose credential authenticates them). Both are read here
so the worker gets the route from the same query that resolves the agent, never
from an ingress request body. ``slack`` never carries an ``endpoint``, because
its route is the worker's configured Slack origin, but it DOES name its bot
identity in ``adapter`` (ADR-0168 decision 3). A turn queued without one means
``default``, read through ``route_identity``, never "no route" the way a
non-Slack NULL pair does. The worker resolves by all three fields: the SQL
selects the pair and ``matching_routes`` (``aci_protocol.turn``) narrows by
identity, so a route bound under one Slack identity does not answer a turn
addressed to another.

A pair that is not bound, or bound but not to this turn's identity, resolves
to None -- a polite drop naming both halves, never a fallback to the address
alone, because that fallback is exactly the silent misroute (#38) this
predicate closes.
"""

from __future__ import annotations

import base64
import json
import logging
import math
import re
import secrets
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import quote

import aiohttp
from aci_protocol import BootEnv, Budget
from aci_protocol.slack_identities import IDENTITY_NAME_MAX_LENGTH, IDENTITY_NAME_PATTERN
from aci_protocol.turn import (
    CLUSTER_MESSAGE_ADAPTER,
    DEFAULT_IDENTITY,
    SLACK_KIND,
    ReplyHandle,
    matching_routes,
)
from curie_internal import sandbox_token
from curie_internal.sealing_key import is_sealing_key_name
from plugin_format import is_reserved_boot_env_name
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from . import caller_token
from .attachments import AgentRoute
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
# CURIE_PLUGIN_DIR (a runner/chart handoff). CURIE_BUNDLE_VERSION is the
# agent-readable version_label of that same bundle (#2174). The rest are the
# frozen ACI SessionConfig env.
BUNDLE_REF_ENV = BootEnv.env_key("bundle_ref")
BUNDLE_VERSION_ENV = BootEnv.env_key("bundle_version")
PLUGIN_DIR_ENV = BootEnv.env_key("plugin_dir")
BUDGET_ENV = BootEnv.env_key("budget")
MAX_TURNS_ENV = BootEnv.env_key("max_turns")
CONNECTOR_CALLER_TOKEN_ENV = BootEnv.env_key("connector_caller_token")
SESSION_ID_ENV = BootEnv.env_key("session_id")
FAKE_MODEL_ENV = BootEnv.env_key("fake_model")
CREDENTIALS_ENV = BootEnv.env_key("credentials_ref")
# The memory port (#264): the API key the runner authenticates with when it
# dereferences the agent's memory namespace on the state API at boot. A scoped
# ``state`` token (ADR-0033), and a runner-local knob rather than part of the
# frozen ACI env, like CURIE_RUNNER_TOKEN. The namespace URL itself rides in
# the frozen SessionConfig's memory_ref, which render_worker emits.
MEMORY_REF_ENV = BootEnv.env_key("memory_ref")
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
REVIEWER_MODEL_ENV = BootEnv.env_key("reviewer_model")
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
# SandboxClaim CR (their secretKeyRef delivery is #1488); the docker substrate
# forwards them directly. The marker and the keys it names are both kept off the
# k8s claim, so a connector secret is never persisted in etcd.
CONNECTOR_SECRET_KEYS_ENV = BootEnv.env_key("connector_secret_keys")
# #430 one-shot post-approval allowance (ADR-0035): a runner-local knob carrying
# the single approved tool name the runner gate lets through once on a resume boot.
GRANT_TOOL_ENV = BootEnv.env_key("approval_grant_tool")
# The paired argument object comes only from the approved permission row.
GRANT_ARGUMENTS_ENV = BootEnv.env_key("approval_grant_arguments")
# #544 Decision A2 turn-end reconciliation marker: an authority-free FACT that
# THIS resume boot is resuming a policy-gate approval. Unlike GRANT_TOOL_ENV it
# confers nothing -- the runner reads it only to decide whether to emit an
# observe-only warning when the approved business action never ran.
RESUMED_KIND_ENV = BootEnv.env_key("approval_resumed_kind")
# #3077 live factory status card: the request-bound report_progress URL and
# scoped token, minted by the kernel's resume overlay per work-item execution.
PROGRESS_URL_ENV = BootEnv.env_key("progress_url")
PROGRESS_TOKEN_ENV = BootEnv.env_key("progress_token")
# ADR 0187 factory issue read: the API route and the execution scoped
# capability naming this execution's WorkItem issue, minted per boot.
ISSUE_READ_URL_ENV = BootEnv.env_key("issue_read_url")
ISSUE_READ_TOKEN_ENV = BootEnv.env_key("issue_read_token")
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
# Cap on a sandbox state token (ADR-0033). The boot-env token no longer uses
# this as its lifetime (#3823); the per-turn memory credential and the
# connector caller token still do.
SANDBOX_TOKEN_TTL_SECONDS = 24 * 60 * 60
# #3823: the boot-env state token outlives the turn by this many seconds, so
# the runner can finish the state calls already in flight when the turn's
# stream deadline lands. It is not a second lifetime.
BOOT_TOKEN_GRACE_SECONDS = 60
# NULL agents.execution_deadline_seconds means this span. The API stamps the
# same number at start (curie_api.models.DEFAULT_EXECUTION_DEADLINE_SECONDS).
DEFAULT_EXECUTION_DEADLINE_SECONDS = 1800
# #3776: the API route a turn's end is reported on, and how long that call may
# take. Short: it runs as each attempt ends, and failing it only leaves the
# credential to expire as it did before.
CLOSED_TURNS_PATH = "/v1/internal/memory/closed-turns"
_CLOSE_TURN_TIMEOUT = aiohttp.ClientTimeout(total=5)
# #3823: the worker reports a sandbox claim's boot credential released.
RELEASED_CREDENTIALS_PATH = "/v1/internal/state/released-credentials"
_RELEASE_CREDENTIAL_TIMEOUT_S = 5
# ADR 0100 (#2877): the API route that mints a turn's channel read capability,
# and how long that call may take. Short: an open mint runs before the runner
# call and a steer mint runs under the per-thread lock, and a failed mint only
# runs the turn without the capability.
CHANNEL_READ_CONTEXT_PATH = "/v1/internal/channel-read/context"
_CHANNEL_READ_CONTEXT_TIMEOUT = aiohttp.ClientTimeout(total=3)
_CHANNEL_READ_GRANT_ABSENT = "channel_read.grant_absent"
_CHANNEL_READ_CODE = re.compile(r"^channel_read\.[a-z_]{1,64}$")
# ADR-0188: the ``sender`` claim of a turn with no person behind it (a job, an
# eval). The runner renders the same string as "no author"
# (``memory_facts.NO_PERSON``); ``tests/test_memory_fact_key_parity.py`` pins
# the two.
NO_PERSON = "<no person>"

# #1909: local/cluster message-path eval stamps this prefix on conversation_id
# so boot_env omits ambient agent memory. Frozen in
# tests/vectors/eval-memory-isolation.json with the CLI copy. Kernel.py is
# not in the loop: it already forwards conversation_id as thread_key.
EVAL_ISOLATE_THREAD_PREFIX = "eval:"

# @spec WORKER-CANARY-2: a declared Slack identity has a narrower shape than
# the general binding adapter slug. Share its frozen name rule.
_SLACK_IDENTITY = re.compile(IDENTITY_NAME_PATTERN)


def _valid_slack_identity(identity: str) -> bool:
    """@spec WORKER-CANARY-2: declared identity shape, excluding the relay name."""

    return (
        identity != CLUSTER_MESSAGE_ADAPTER
        and len(identity) <= IDENTITY_NAME_MAX_LENGTH
        and _SLACK_IDENTITY.fullmatch(identity) is not None
    )


def binding_adapter_for_handle(handle: ReplyHandle) -> str | None:
    """@spec WORKER-CANARY-1 WORKER-CANARY-2: select the binding, not egress."""

    if handle.kind != SLACK_KIND or handle.adapter != CLUSTER_MESSAGE_ADAPTER:
        return handle.adapter
    identity = handle.identity
    if identity is None:
        return DEFAULT_IDENTITY
    if not _valid_slack_identity(identity):
        raise ValueError("cluster-message identity must name a declared Slack identity")
    return identity


def _valid_slack_selector(kind: str, adapter: str | None) -> bool:
    """@spec WORKER-CANARY-2: never treat an invalid selector as default."""

    return (
        kind != SLACK_KIND
        or adapter is None
        or adapter == CLUSTER_MESSAGE_ADAPTER
        or _valid_slack_identity(adapter)
    )


def is_eval_isolate_thread(thread_key: str) -> bool:
    """True when ``thread_key`` is a hermetic local/cluster eval conversation.

    The CLI stamps ``EVAL_ISOLATE_THREAD_PREFIX`` onto each eval case's
    ``QueuedTurn.conversation_id`` (#1909). That value is this method's
    ``thread_key``. Prefix match only: a thread that merely starts with
    ``eval-`` is a normal mention and still loads durable memory.
    """

    return thread_key.startswith(EVAL_ISOLATE_THREAD_PREFIX)


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


# Migration 0051's auto-provisioned tenant, the one every row belongs to until
# a second tenant exists. A frozen copy: this package does not import the API's.
DEFAULT_TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")

# The trailing `d.id DESC` carries no meaning of its own -- id order is not a
# precedence rule and nothing may start reading one into it. It exists only to
# make the order TOTAL: two active deployments in the same environment with an
# identical `deployed_at` leave the first two keys tied, and an undefined tie
# lets this query and connector_loop.py's _TARGETS_SQL -- different joins,
# different plans -- pick different winners for the same agent. Since #1216 that
# disagreement costs a DESTRUCTIVE prune (the reconciler removes the connector
# objects of the version the sandbox is actually booting), not merely a stale
# apply, so the key is duplicated verbatim in both statements.
_RESOLVE_SQL = """
SELECT a.id AS agent_id,
       a.name AS agent_name,
       a.max_usd_per_day AS max_usd_per_day,
       a.max_output_tokens_per_run AS max_output_tokens_per_run,
       a.behavior_packs AS behavior_packs,
       a.model AS model,
       a.reviewer_model AS reviewer_model,
       a.thinking AS thinking,
       a.approval_required_tools AS approval_required_tools,
       a.approval_routes AS approval_routes,
       a.secrets AS secrets,
       d.id AS deployment_id,
       d.workspace_enabled AS workspace_enabled,
       d.environment AS deployment_environment,
       a.memory AS memory,
       v.id AS version_id,
       v.version_label AS version_label,
       v.bundle_ref AS bundle_ref,
       c.kind AS kind,
       c.address AS address,
       c.endpoint AS endpoint,
       c.adapter AS adapter
FROM {schema}.agents a
JOIN {schema}.agent_channels c ON c.agent_id = a.id
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
WHERE c.kind = :kind AND c.address = :address
  AND a.tenant_id = :tenant_id AND c.tenant_id = :tenant_id
ORDER BY (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
"""

# A targetless cron turn (#2963) is routed by the hook run's agent, not by a
# binding, so this selects the same active deployment as _RESOLVE_SQL (the
# ORDER BY key is duplicated verbatim for the same tiebreak reason) with no
# agent_channels join. endpoint/adapter are NULL: there is no reply route.
_RESOLVE_AGENT_SQL = """
SELECT a.id AS agent_id,
       a.name AS agent_name,
       a.max_usd_per_day AS max_usd_per_day,
       a.max_output_tokens_per_run AS max_output_tokens_per_run,
       a.behavior_packs AS behavior_packs,
       a.model AS model,
       a.reviewer_model AS reviewer_model,
       a.thinking AS thinking,
       a.approval_required_tools AS approval_required_tools,
       a.approval_routes AS approval_routes,
       a.secrets AS secrets,
       d.id AS deployment_id,
       d.workspace_enabled AS workspace_enabled,
       d.environment AS deployment_environment,
       a.memory AS memory,
       v.id AS version_id,
       v.version_label AS version_label,
       v.bundle_ref AS bundle_ref,
       NULL AS endpoint,
       NULL AS adapter
FROM {schema}.agents a
JOIN {schema}.deployments d ON d.agent_id = a.id AND d.status = 'active'
JOIN {schema}.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
WHERE a.id = :agent_id AND a.tenant_id = :tenant_id
ORDER BY (d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC
LIMIT 1
"""

# ``resolve`` deliberately requires an active deployment: it is the only
# deployment a worker may boot. When that lookup misses, the kernel needs this
# narrower second query to distinguish an unbound route from an existing agent
# that has not been deployed yet (#1522). It is never a routing fallback.
_UNDEPLOYED_BINDING_SQL = """
SELECT a.id AS agent_id,
       a.name AS agent_name,
       c.kind AS kind,
       c.address AS address,
       c.endpoint AS endpoint,
       c.adapter AS adapter
FROM {schema}.agents a
JOIN {schema}.agent_channels c ON c.agent_id = a.id
WHERE c.kind = :kind AND c.address = :address
  AND a.tenant_id = :tenant_id AND c.tenant_id = :tenant_id
"""

# ADR-0168 decision 6: the identity bound at an address, if any. A channel-port
# identity is one adapter deployment on one address and writes as that
# address. Case-insensitive because the mail adapter lowercases a sender
# (`curie_mail_adapter.adapter._bare_address`).
_ADDRESS_IDENTITY_SQL = """
SELECT c.adapter AS adapter
FROM {schema}.agent_channels c
WHERE c.kind = :kind AND lower(c.address) = lower(:address) AND c.adapter IS NOT NULL
  AND c.tenant_id = :tenant_id
ORDER BY c.adapter
LIMIT 1
"""


class ResolvedDeployment(BaseModel):
    """The agent binding for a channel: which version to run and its budget."""

    agent_id: uuid.UUID
    # The agent's NAME, not just its id: connector object names are agent-scoped
    # (#1116) and use the name, so the runner needs it to derive a URL that
    # matches the Service that exists.
    agent_name: str
    # The active deployment is the sole selector the trusted worker may redeem
    # through the internal workspace credential endpoint.  Optional defaults
    # keep old worker doubles and rolling-deploy rows source-compatible.
    deployment_id: uuid.UUID | None = None
    # Retained on the wire-facing deployment model for compatibility only. The
    # kernel deliberately does not use this legacy per-deployment bit as coding
    # enablement; CURIE_WORKSPACE_ENABLED remains the worker-wide kill switch.
    workspace_enabled: bool = False
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
    # The agent's reviewer override (#4120). None lets the runner choose its
    # credential's provider default.
    reviewer_model: str | None = None
    # The agent's thinking depth (#1182, ADR-0098), forwarded as CURIE_THINKING
    # at boot. None falls back to the worker's configured default; unset at both
    # layers sends nothing and leaves the model's own default standing.
    thinking: str | None = None
    # Per-agent runner resources (#3209). None means the chart block. A set
    # value is applied to the next claim, not to a sandbox that is already running.
    runner_resources: dict[str, Any] | None = None
    # The active deployment's environment (#3166), forwarded as
    # CURIE_DEPLOYMENT_ENVIRONMENT so runner traces carry it. Optional so old
    # worker doubles stay source-compatible.
    deployment_environment: str | None = None
    # The agent's permission gates (#245): tool names requiring human approval,
    # forwarded as CURIE_APPROVAL_REQUIRED_TOOLS at boot. None means no gates.
    approval_required_tools: list[str] | None = None
    # The agent's approval route bindings (#247): manifest route name ->
    # binding with one verified-card resolution target and an optional
    # text-only notification target. None means no bindings.
    approval_routes: dict[str, Any] | None = None
    # The agent's connector secrets (ADR-0009, #429): env-var name -> secret
    # value, injected by name into the sandbox boot env so a bundle's authed MCP
    # server can read its token via `.mcp.json` `${VAR}` expansion. None means no
    # connector secrets. (Local tier stores values on the agent row; the cluster
    # tier delivers them via a per-agent K8s Secret instead.)
    secrets: dict[str, str] | None = None
    # The binding row's server-controlled reply route (ADR-0096 phase 2).
    # `endpoint` is the channel API base URL this kind's replies go back
    # through. `adapter` is the egress adapter identity whose credential
    # authenticates them for a non-Slack kind, and, for `slack`, the bot
    # IDENTITY this route names (ADR-0168 decision 3). `slack` carries no
    # `endpoint`, because its route is the worker's configured Slack origin;
    # any other kind sets both or neither (`agent_channels_route_ck`).
    endpoint: str | None = None
    adapter: str | None = None
    # Whether this agent's bindings share one general-state namespace, or each
    # get their own (#1525 follow-up). Read fresh per resolve, so flipping it
    # takes effect on the very next turn -- there is no cached copy anywhere
    # to go stale.
    memory: bool = False
    # Whether the operator turned memory writes on for this agent (#1461,
    # ADR-0167). A bound turn's runner gets its channel memory ref either way,
    # so stored facts stay readable (#3621); this flag rides with it as
    # CURIE_MEMORY_WRITES and decides only whether the remember/update/forget
    # tools mount. Off is the default.
    # Not selected by the resolver statements: the column arrives in migration
    # 0068 and resolution runs against older schemas, so the kernel reads it
    # with ``memory_writes_for`` and copies it on, as with runner_resources.
    memory_writes: bool = False


class AmbiguousRoute(RuntimeError):
    """A turn names no adapter on a non-Slack pair that several agents bind.

    An omitted non-Slack adapter selects every route on the pair
    (``matching_routes``), and migration 0070's triple key lets two agents hold
    one pair under different adapters, so no deployment is this turn's. The
    worker's twin of the API's ``crud.AmbiguousRoute``: never resolved by
    picking one, because the pick runs one agent's turn under another's
    deployment, secrets and reply route (ADR-0168 decision 3, #38).
    """

    def __init__(self, kind: str, address: str, agent_ids: Sequence[uuid.UUID]) -> None:
        self.kind = kind
        self.address = address
        self.agent_ids = sorted(str(agent_id) for agent_id in agent_ids)
        super().__init__(
            f"{len(self.agent_ids)} agents are bound to {kind}:{address} and the turn "
            f"names no adapter to choose one ({', '.join(self.agent_ids)})"
        )


def refuse_several_agents(kind: str, address: str, rows: Sequence[Any]) -> None:
    """Raise ``AmbiguousRoute`` when ``rows`` belong to more than one agent.

    Counts distinct agents, not rows: one agent's several rows are one
    deployment whichever row answers.
    """

    agents = {row["agent_id"] for row in rows}
    if len(agents) > 1:
        raise AmbiguousRoute(kind, address, list(agents))


class BoundAgent(BaseModel):
    """An agent bound to a route but without a deployable active version."""

    agent_id: uuid.UUID
    agent_name: str
    endpoint: str | None = None
    adapter: str | None = None


def warn_if_multiple_agents_bound(kind: str, address: str, rows: Sequence[Any]) -> None:
    """Warn when a binding resolves to more than one agent, naming the shadowed.

    Takes the routing PAIR, not a pre-joined string: the pair is what the
    resolver looked up, and an address means nothing without the kind it was
    bound under (ADR-0096 -- the same address can legitimately exist under two
    kinds).

    The ORDER BY picks one deterministic winner (prod-first, then most recent).
    The API holds one agent per route (``agent_channels_route_key``, migration
    0070, superseding 0023's pair key, 0021's address-only
    ``agent_channels_address_key`` and 0017's ``agents_slack_channel_key``), so
    a Slack turn, or a turn that names its adapter, cannot reach this state
    through the write paths. A non-Slack turn that omits its adapter selects
    every route on the pair, and ``resolve`` refuses it with ``AmbiguousRoute``
    before this runs when those routes belong to several agents. So this is
    reachable only through rows written out of band, and stays as defense in
    depth there, because silently shadowing an agent is the failure mode #38
    existed to kill.

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


def _deployment_from_row(data: dict[str, Any]) -> ResolvedDeployment:
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
    runner_resources = data.get("runner_resources")
    if isinstance(runner_resources, str):
        data["runner_resources"] = json.loads(runner_resources)
    return ResolvedDeployment.model_validate(data)


@dataclass(frozen=True)
class ChannelReadMint:
    """One minted channel read capability (ADR 0100, #2877).

    ``turn_key`` is the logical turn digest the API resolved, approval chain
    hops included, so the kernel addresses the shared Valkey ledger keys for
    revocation without resolving the chain itself. The token never appears in
    ``repr``."""

    token: str = field(repr=False)
    generation: int
    expires_at: int
    turn_key: str


class ChannelReadGrantAbsent(Exception):
    """The deployment's bundle does not grant channel read (409 ``grant_absent``).

    Raised, not returned as None, so the kernel can remember the deployment
    and stop asking; every other refusal or failure is None."""


class BindingResolver:
    """Resolves a channel address to its active agent deployment (read-only).

    Scoped to one tenant (ADR 0166 decision 3). Every read matches only agents
    whose ``tenant_id`` is the resolver's, and a route also requires the
    binding row to carry it, so a binding out of step with its agent fails
    closed. Another tenant's agent answers exactly as an unknown one does.
    ``deployments`` and ``agent_versions`` are row scoped by the column only and
    follow their agent (``v.agent_id = a.id``); predicates on them would have to
    land in ``connector_loop._TARGETS_SQL`` too, or the two rankings diverge
    (#3049). The tenant is the default one until the queued turn carries its
    own (#2914); it is deliberately not an operator setting, since a mistyped
    tenant would drop every event.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        config: WorkerConfig,
        *,
        tenant_id: uuid.UUID = DEFAULT_TENANT_ID,
    ) -> None:
        self._engine = engine
        self._config = config
        self._tenant_id = tenant_id
        # Table identifiers are not user input; the schema comes from config.
        self._sql = text(_RESOLVE_SQL.format(schema=config.db_schema))
        self._undeployed_binding_sql = text(_UNDEPLOYED_BINDING_SQL.format(schema=config.db_schema))
        self._resolve_agent_sql = text(_RESOLVE_AGENT_SQL.format(schema=config.db_schema))
        self._address_identity_sql = text(_ADDRESS_IDENTITY_SQL.format(schema=config.db_schema))

    async def resolve(
        self, kind: str, adapter: str | None, address: str
    ) -> ResolvedDeployment | None:
        """Resolve a turn's route TRIPLE ``(kind, adapter, address)`` to its
        active deployment.

        It resolves by all three fields (ADR-0168 decision 3): ``kind`` and
        ``address`` select the SQL rows, and ``matching_routes`` narrows them in
        Python by the turn's ``adapter``, so a row bound under one identity
        does not answer a turn addressed to another on a pair several
        identities share. All three are required and none has a default: an omitted
        identity or an address-only overload would silently answer with
        whichever row happened to be bound, which is #38's misroute wearing a
        new hat.

        An omitted non-Slack adapter selects every route on the pair, so when
        those routes belong to several agents this raises ``AmbiguousRoute``
        rather than answer. That is judged over every BINDING on the pair, not
        only the deployed ones this query returns: an undeployed agent's
        route-less binding would otherwise leave the other agent's row as the
        only match, and the turn would run as that agent.
        """
        # @spec WORKER-CANARY-2: direct resolver callers also fail closed.
        if not _valid_slack_selector(kind, adapter):
            return None
        params = {"kind": kind, "address": address, "tenant_id": self._tenant_id}
        async with self._engine.connect() as conn:
            if adapter is None and kind != SLACK_KIND:
                bound = (await conn.execute(self._undeployed_binding_sql, params)).all()
                refuse_several_agents(kind, address, [dict(row._mapping) for row in bound])
            result = await conn.execute(self._sql, params)
            rows = result.all()
        matches = matching_routes(rows, kind, address, adapter)
        if not matches:
            return None
        mapped = [dict(row._mapping) for row in matches]
        warn_if_multiple_agents_bound(kind, address, mapped)
        return _deployment_from_row(mapped[0])

    async def resolve_agent(self, agent_id: uuid.UUID) -> ResolvedDeployment | None:
        """Resolve an explicit agent to its active deployment, with no binding.

        Only for a targetless cron turn (#2963), whose agent is the hook run
        row's validated id. Never a fallback for a binding miss.
        """
        async with self._engine.connect() as conn:
            result = await conn.execute(
                self._resolve_agent_sql, {"agent_id": agent_id, "tenant_id": self._tenant_id}
            )
            row = result.mappings().first()
        return None if row is None else _deployment_from_row(dict(row))

    async def undeployed_binding(
        self, kind: str, adapter: str | None, address: str
    ) -> BoundAgent | None:
        """Return a bound agent after ``resolve`` found no active deployment.

        Narrows by the same route triple ``resolve`` does (``matching_routes``,
        ADR-0168 decision 3): a bound-but-undeployed diagnostic for a
        DIFFERENT identity on this pair must not read as this turn's agent.

        The caller must use this only as a diagnostic after an active-resolution
        miss. Returning this record never grants a runner boot: a route remains
        runnable only through ``ResolvedDeployment`` above.
        """
        # @spec WORKER-CANARY-2: the diagnostic cannot name default either.
        if not _valid_slack_selector(kind, adapter):
            return None
        async with self._engine.connect() as conn:
            result = await conn.execute(
                self._undeployed_binding_sql,
                {"kind": kind, "address": address, "tenant_id": self._tenant_id},
            )
            rows = result.all()
        matches = matching_routes(rows, kind, address, adapter)
        if not matches:
            return None
        mapped = [dict(row._mapping) for row in matches]
        # The same refusal as ``resolve``: the diagnostic reply goes out
        # through the bound agent's route, so naming either agent is a guess.
        refuse_several_agents(kind, address, mapped)
        return BoundAgent.model_validate(mapped[0])

    async def identity_for_address(self, kind: str, address: str) -> str | None:
        """The identity bound at ``address`` on ``kind``, or None (ADR-0168 decision 6)."""
        async with self._engine.connect() as conn:
            result = await conn.execute(
                self._address_identity_sql,
                {"kind": kind, "address": address, "tenant_id": self._tenant_id},
            )
            value = result.scalar()
        return value if isinstance(value, str) and value else None

    async def routes_for_agent(self, agent_id: uuid.UUID) -> list[AgentRoute]:
        """The agent's bindings as they are NOW (ADR 0205 decision 5).

        A re-fetch of an earlier attachment resolves its route from these and
        never from anything recorded with the file, so a removed binding makes
        the file unavailable and a moved adapter endpoint is followed. Read
        only; the address is not returned because no re-fetch needs it.
        """

        sql = text(
            f"SELECT kind, adapter, endpoint FROM {self._config.db_schema}.agent_channels "
            "WHERE agent_id = :id AND tenant_id = :tenant_id "
            "ORDER BY kind, adapter NULLS FIRST, endpoint NULLS FIRST"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
            rows = result.all()
        return [
            AgentRoute(kind=str(row.kind), adapter=row.adapter, endpoint=row.endpoint)
            for row in rows
        ]

    async def repo_full_name(self, agent_id: uuid.UUID) -> str | None:
        """The agent's GitHub repo (owner/name), for the eval PR-check report."""
        sql = text(
            f"SELECT repo_full_name FROM {self._config.db_schema}.agents "
            "WHERE id = :id AND tenant_id = :tenant_id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
            row = result.first()
        if row is None:
            return None
        value: str | None = row[0]
        return value

    async def approval_grant_tool(self, event_id: str, agent_id: uuid.UUID) -> str | None:
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
        tool = summary[len(_PERMISSION_GATE_SUMMARY_PREFIX) :].split(" ", 1)[0]
        return tool or None

    async def approval_grant_arguments(
        self, event_id: str, agent_id: uuid.UUID
    ) -> dict[str, Any] | None:
        """Return the denied call's stored arguments for its approved resume.

        The new carrier has no summary fallback. Only an approved permission
        gate with a stored tool and a resume for the same agent may carry arguments;
        policy approvals, old rows and malformed values return None. An empty
        object remains a valid argument value.
        """
        approval_id = _parse_resume_event_id(event_id)
        if approval_id is None:
            return None
        sql = text(
            f"SELECT status, agent_id, gate_kind, granted_tool, granted_arguments "
            f"FROM {self._config.db_schema}.approvals WHERE id = :id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": approval_id})
            row = result.mappings().first()
        if row is None:
            return None
        if row["status"] != "approved":
            return None
        row_agent_id = row["agent_id"]
        if row_agent_id is None or row_agent_id != agent_id:
            return None
        if row["gate_kind"] != "permission" or not row["granted_tool"]:
            return None
        arguments: Any = row["granted_arguments"]
        return arguments if isinstance(arguments, dict) else None

    async def approval_resumed_kind(self, event_id: str, agent_id: uuid.UUID) -> str | None:
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
        sql = text(
            f"SELECT secrets FROM {self._config.db_schema}.agents "
            "WHERE id = :id AND tenant_id = :tenant_id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
            row = result.first()
        if row is None or row[0] is None:
            return None
        value = row[0]
        if isinstance(value, str):
            value = json.loads(value)
        return value if isinstance(value, dict) else None

    async def name_for(self, agent_id: uuid.UUID) -> str | None:
        """The agent's NAME for eval pool routing (#1488). None if unknown."""
        sql = text(
            f"SELECT name FROM {self._config.db_schema}.agents "
            "WHERE id = :id AND tenant_id = :tenant_id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
            row = result.first()
        if row is None:
            return None
        value: str | None = row[0]
        return value

    async def runner_resources_for(self, agent_id: uuid.UUID) -> dict[str, Any] | None:
        """The agent's runner resource override, or None for the chart block.

        This is a separate read from deployment resolution. Resolution runs in
        migration tests against schemas that predate the column.
        """
        sql = text(
            f"SELECT runner_resources FROM {self._config.db_schema}.agents "
            "WHERE id = :id AND tenant_id = :tenant_id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
            row = result.first()
        if row is None:
            return None
        value = row[0]
        if isinstance(value, str):
            value = json.loads(value)
        return value if isinstance(value, dict) else None

    async def memory_writes_for(self, agent_id: uuid.UUID) -> bool:
        """Whether the operator turned memory writes on for the agent (#1461).

        A separate read from deployment resolution, like
        ``runner_resources_for``: resolution runs in migration tests against
        schemas that predate the column (migration 0068). A missing agent row or
        a null value reads as off.
        """
        sql = text(
            f"SELECT memory_writes FROM {self._config.db_schema}.agents "
            "WHERE id = :id AND tenant_id = :tenant_id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
            row = result.first()
        return bool(row is not None and row[0])

    async def execution_deadline_seconds_for(self, agent_id: uuid.UUID) -> int:
        """The agent's work item execution span, or the platform default.

        A separate read, like ``memory_writes_for``, so a schema from before
        the column still boots. A missing row, a null, or a read error uses
        ``DEFAULT_EXECUTION_DEADLINE_SECONDS``.
        """

        sql = text(
            f"SELECT execution_deadline_seconds FROM {self._config.db_schema}.agents "
            "WHERE id = :id AND tenant_id = :tenant_id"
        )
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
                row = result.first()
        except Exception:  # noqa: BLE001 - a missing column still boots
            logger.warning(
                "execution deadline read failed agent=%s; using the platform default",
                agent_id,
            )
            return DEFAULT_EXECUTION_DEADLINE_SECONDS
        if row is None or row[0] is None:
            return DEFAULT_EXECUTION_DEADLINE_SECONDS
        return int(row[0])

    async def model_settings_for(
        self, agent_id: uuid.UUID
    ) -> tuple[str | None, str | None, str | None, dict[str, Any] | None]:
        """Model, reviewer model, thinking, and resources for eval boots."""
        sql = text(
            "SELECT model, reviewer_model, thinking, runner_resources "
            f"FROM {self._config.db_schema}.agents "
            "WHERE id = :id AND tenant_id = :tenant_id"
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(sql, {"id": agent_id, "tenant_id": self._tenant_id})
            row = result.first()
        if row is None:
            return None, None, None, None
        model: str | None = row[0]
        reviewer_model: str | None = row[1]
        thinking: str | None = row[2]
        runner_resources = row[3]
        if isinstance(runner_resources, str):
            runner_resources = json.loads(runner_resources)
        if runner_resources is not None and not isinstance(runner_resources, dict):
            runner_resources = None
        return model, reviewer_model, thinking, runner_resources

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

    def boot_env(
        self,
        resolved: ResolvedDeployment,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
        isolate_memory: bool = False,
        caller_run: str | None = None,
        caller_work_item: str | None = None,
        caller_exp_ceiling: int | None = None,
        token_ttl_s: float | None = None,
    ) -> dict[str, str]:
        """The env injected into the sandbox claim for a bound run.

        Rendered from the declared contract (``BootEnv``, #488/ADR-0049) rather
        than a hand-built dict, so every name here is typed once, in one place,
        and a rename cannot leave the sandbox booting fine with a silently
        dropped feature. ``render_worker`` emits only the worker-authoritative
        keys: it never writes CURIE_SANDBOX_ID or CURIE_RUNNER_PORT, which
        the substrate derives from the pod itself.

        An eval-isolated turn (#1909) omits ``CURIE_MEMORY_REF`` /
        ``CURIE_MEMORY_TOKEN`` so default local/cluster eval does not load
        ambient durable agent memory. Direct callers may still express that
        intent with the legacy eval-prefixed ``thread_key``; the kernel passes
        ``isolate_memory`` explicitly because its internal key also includes
        the channel kind and address.
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
        #
        # Narrowed to THIS binding's own path segment, unless the agent has
        # opted every binding into one shared namespace, or this caller has no
        # binding to name (#1525 follow-up): a memory=False agent's bundle
        # composes ``/<namespace>/<key>`` onto whichever base it was handed
        # here, unaware which shape it got. For general state the scoping
        # decision lives in which URL the worker minted: a partition key within
        # the agent's own store, which the API checks against
        # ``agent_channels``. Memory is different (ADR-0188, which reverses the
        # #1525 follow-up's rejection of a binding claim for memory): one
        # channel's memory can hold a direct message, so the state token below
        # carries a ``binding`` claim and the API holds it to that channel's
        # memory. Agent memory and history stay agent-wide either way; channel
        # memory (below) is binding-scoped by design (ADR-0167, #1461) and is
        # decided separately from this ``memory`` flag.
        state_url = f"{base}/agents/{resolved.agent_id}/state"
        if not resolved.memory and kind is not None and address is not None:
            state_url = (
                f"{base}/agents/{resolved.agent_id}/state/bindings/"
                f"{quote(kind, safe='')}/{quote(address, safe='')}"
            )
        # Channel memory (#1461, ADR-0167): the agent's memory namespace scoped
        # to this turn's binding, on the same store. The boot-env memory token
        # only READS it (ADR-0188); a write needs the per-turn credential from
        # ``turn_memory_token``, which rides the turn's ACI ``Event`` and never
        # this env. Reading channel memory needs no switch, so
        # the ref is set whenever the turn names a binding (#3621). Whether the
        # agent may save to it is a separate flag, ``memory_writes`` (#3659),
        # sent explicitly alongside the ref so the runner mounts the remember,
        # update and forget tools only when the operator turned writes on. An
        # eval-isolated turn carries no memory at all, so it gets neither.
        channel_memory_ref: str | None = None
        if (
            kind is not None
            and address is not None
            and not (isolate_memory or is_eval_isolate_thread(thread_key))
        ):
            channel_memory_ref = (
                f"{base}/agents/{resolved.agent_id}/state/bindings/"
                f"{quote(kind, safe='')}/{quote(address, safe='')}/memory"
            )
        # Mint scoped tokens (ADR-0033, #410) for this agent. Two scopes, because
        # the memory/history loaders and the bundle reach DIFFERENT namespaces:
        #  - the broad ``state`` token backs the memory and history tokens, whose
        #    loaders MUST read the reserved ``memory`` namespace and read/write
        #    ``transcript`` to rehydrate the agent across suspend/resume. Its
        #    ADR-0188 claims narrow it on memory: ``binding`` names the one
        #    channel whose memory it reaches (JSON null for a turn with no
        #    channel), and ``memory: "read"`` makes it read-only there;
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
        # #3823: both boot-env state tokens share one credential id and the
        # turn deadline plus a short grace. The caller token keeps its own
        # lifetime (ADR 0178): the 24 hour cap, or the run deadline when one
        # was supplied.
        state_exp = _boot_state_token_exp(token_ttl_s)
        credential_id = uuid.uuid4().hex
        if self._config.api_key:
            state_token = sandbox_token.mint(
                self._config.api_key,
                agent=str(resolved.agent_id),
                scope="state",
                exp=state_exp,
                claims={
                    "binding": _binding_claim(kind, address),
                    "memory": "read",
                    "cred": credential_id,
                },
            )
            app_state_token = sandbox_token.mint(
                self._config.api_key,
                agent=str(resolved.agent_id),
                scope="state.app",
                exp=state_exp,
                claims={"cred": credential_id},
            )
        # The caller token (ADR-0168 decision 7, ADR 0178): this sandbox's
        # agent, signed for its hosted connectors. Its expiry stays the 24 hour
        # cap, or the run's execution deadline when the kernel passed one. It
        # does not follow the boot state token (#3823). No key mints none,
        # which is the stock install. render_worker emits it only with the
        # connector scope.
        connector_caller_token: str | None = None
        if self._config.connector_caller_signing_key.strip():
            caller_exp = int(time.time()) + SANDBOX_TOKEN_TTL_SECONDS
            if caller_exp_ceiling is not None:
                caller_exp = min(caller_exp, int(caller_exp_ceiling))
            connector_caller_token = caller_token.mint(
                self._config.connector_caller_signing_key,
                agent=resolved.agent_name,
                exp=caller_exp,
                run=caller_run,
                work_item=caller_work_item,
            )
        env = BootEnv.render_worker(
            plugin_dir=self._config.bundle_plugin_dir,
            session_id=f"agent-{resolved.agent_id}-thread-{thread_key}",
            budget=self.budget_for(resolved),
            memory_ref=memory_ref,
            history_ref=history_ref,
            bundle_ref=resolved.bundle_ref,
            # Agent-readable identity (#2174): the platform-tracked
            # version_label, not the RustFS object key. Blank is omitted.
            bundle_version=resolved.version_label or None,
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
            connector_caller_token=connector_caller_token,
            # The agent's pinned model (#254) overrides the worker default; None
            # falls back to the platform default.
            model=resolved.model if resolved.model is not None else self._config.model,
            reviewer_model=resolved.reviewer_model,
            # Same precedence as the model above (#1182): the agent's value wins,
            # then the platform default, then nothing at all -- and "nothing at
            # all" is what makes an unconfigured install behave as it always has.
            thinking=(resolved.thinking if resolved.thinking is not None else self._config.thinking)
            or None,
            deployment_environment=resolved.deployment_environment or None,
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
            channel_memory_ref=channel_memory_ref,
            # The writes switch rides only with a channel ref, as an explicit
            # bool; without a ref there is nothing to write to.
            memory_writes=(
                bool(resolved.memory_writes) if channel_memory_ref is not None else None
            ),
            # The general state store exposed to bundle code (#249): the NARROW
            # ``state.app`` token authorizes the URL -- refused on the reserved
            # memory/transcript namespaces server-side -- so the token is omitted
            # (and the URL still emitted) on the no-key fake/local path.
            state_url=state_url,
            state_token=app_state_token,
            channel_bound=True if kind and address else None,
            channel_kind=kind if kind else None,
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
        # CURIE_CONNECTOR_SECRET_KEYS (their secretKeyRef delivery is #1488).
        # Runs AFTER the render so the reserved-name filter sees the rendered
        # keys, and stays the marker's sole writer -- see the
        # inject_connector_secrets docstring for the #457/#429 rationale.
        inject_connector_secrets(env, resolved.secrets, agent_label=resolved.agent_id)
        # #1909: default local/cluster eval is a static bundle-plus-cases gate.
        # Ambient durable memory is per-agent, so a fresh thread still loaded
        # it and could change a committed case. The CLI marks those turns with
        # EVAL_ISOLATE_THREAD_PREFIX; drop the memory ref/token so the runner
        # boots NullMemoryStore. History stays: each case already has a unique
        # empty thread (#550 / ADR-0051). The kernel carries this intent
        # explicitly once it scopes the internal key by channel.
        if isolate_memory or is_eval_isolate_thread(thread_key):
            env.pop(MEMORY_REF_ENV, None)
            env.pop(MEMORY_TOKEN_ENV, None)
            logger.info("eval isolate: omitted memory_ref for thread %s", thread_key)
        return env

    def turn_memory_token(
        self,
        resolved: ResolvedDeployment,
        *,
        kind: str | None,
        address: str | None,
        thread_key: str,
        sender: str,
        turn: str,
        ttl_s: float,
    ) -> str | None:
        """The per-turn memory write credential (ADR-0188), or None.

        Sent as the turn's ``Event.memory_token`` (MEMORY-TOKEN-2), never in the
        boot env: the boot-env token outlives the turn and so cannot name its
        sender. A ``scope="state"`` token with ``{binding, memory: "write",
        sender, turn}``; the API writes only fact keys with it, only on the
        channel ``binding`` names, and stamps ``sender`` as the fact's author.

        None unless there is a key to sign with, the agent has memory writes on,
        the turn names a binding, and the thread is not eval-isolated (#1909).
        The expiry is ``ttl_s``, the turn's own deadline, capped at the boot
        token's lifetime, with no grace: the credential ends with the turn.
        """

        if not self._config.api_key or not resolved.memory_writes:
            return None
        binding = _binding_claim(kind, address)
        if binding is None or is_eval_isolate_thread(thread_key):
            return None
        lifetime = min(math.ceil(max(0.0, ttl_s)), SANDBOX_TOKEN_TTL_SECONDS)
        exp = int(time.time()) + lifetime
        return sandbox_token.mint(
            self._config.api_key,
            agent=str(resolved.agent_id),
            scope="state",
            exp=exp,
            claims={
                "binding": binding,
                "memory": "write",
                "sender": sender.strip() or NO_PERSON,
                "turn": turn,
            },
        )

    async def close_turn_memory(self, agent_id: uuid.UUID, turn: str) -> None:
        """Tell the API a turn has ended, so its memory write credential is
        refused from now on rather than at its expiry (#3776).

        The resolver mints the credential (``turn_memory_token``), so it retires
        it, on the internal route with the worker token every other
        ``/v1/internal`` call uses. Never raises: a failed close leaves the
        credential to expire at the turn's deadline, as before. An older API
        without the route answers 404, which is logged once, not as an error.
        """

        url = f"{self._config.api_base_url.rstrip('/')}{CLOSED_TURNS_PATH}"
        try:
            async with (
                aiohttp.ClientSession(timeout=_CLOSE_TURN_TIMEOUT) as session,
                session.post(
                    url,
                    json={"agent_id": str(agent_id), "turn": turn},
                    headers={"X-Curie-Worker-Token": self._config.internal_worker_token},
                ) as response,
            ):
                status = response.status
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            logger.warning("could not report memory turn %s as ended: %s", turn, type(exc).__name__)
            return
        if status == 404:
            if not getattr(self, "_closed_turns_route_missing", False):
                self._closed_turns_route_missing = True
                logger.info(
                    "the API has no closed-turns route (404); memory credentials "
                    "stay usable until they expire"
                )
            return
        if not 200 <= status < 300:
            logger.warning("the API refused to end memory turn %s: HTTP %s", turn, status)

    def release_boot_credential_sync(self, agent_id: str, credential: str) -> bool:
        """Tell the API this sandbox's boot credential is released (#3823).

        Called from the substrate when the claim that holds the token is
        deleted. Never raises: a failed report leaves the token to expire at
        the turn deadline plus grace. An older API without the route answers
        404, logged once.
        """

        url = f"{self._config.api_base_url.rstrip('/')}{RELEASED_CREDENTIALS_PATH}"
        body = json.dumps({"agent_id": agent_id, "credential": credential}).encode()
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Curie-Worker-Token": self._config.internal_worker_token,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=_RELEASE_CREDENTIAL_TIMEOUT_S) as response:
                status = response.status
        except urllib.error.HTTPError as exc:
            status = exc.code
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            logger.warning(
                "could not report sandbox credential %s as released: %s",
                credential,
                type(exc).__name__,
            )
            return False
        if status == 404:
            if not getattr(self, "_released_credentials_route_missing", False):
                self._released_credentials_route_missing = True
                logger.info(
                    "the API has no released-credentials route (404); boot "
                    "tokens stay usable until they expire"
                )
            return False
        if not 200 <= status < 300:
            logger.warning(
                "the API refused to release sandbox credential %s: HTTP %s",
                credential,
                status,
            )
            return False
        return True

    async def channel_read_context(
        self,
        *,
        agent_id: uuid.UUID,
        deployment_id: uuid.UUID,
        event_id: str,
        mode: Literal["open", "steer"],
        owner: str | None = None,
        default: tuple[str, str] | None,
        ttl_s: int,
    ) -> ChannelReadMint | None:
        """Mint a turn's channel read capability on the API (ADR 0100, #2877).

        ``mode="open"`` opens the logical turn under ``owner`` (the attempt's
        id); ``mode="steer"`` renews the live one and sends no owner, since the
        API keeps the opener's. Uses the worker token every other
        ``/v1/internal`` call uses, never the platform key.

        Raises ``ChannelReadGrantAbsent`` on 409 ``channel_read.grant_absent``.
        Every other refusal, error, malformed body or unreachable API returns
        None, so the turn runs without the capability (fail closed). An older
        API without the route answers 404, logged once. Neither the token nor
        any response text reaches a log line.
        """

        body: dict[str, Any] = {
            "agent_id": str(agent_id),
            "deployment_id": str(deployment_id),
            "event_id": event_id,
            "mode": mode,
            "default_channel": (
                None if default is None else {"kind": default[0], "address": default[1]}
            ),
            "ttl_s": ttl_s,
        }
        if owner is not None:
            body["owner"] = owner
        url = f"{self._config.api_base_url.rstrip('/')}{CHANNEL_READ_CONTEXT_PATH}"
        try:
            async with (
                aiohttp.ClientSession(timeout=_CHANNEL_READ_CONTEXT_TIMEOUT) as session,
                session.post(
                    url,
                    json=body,
                    headers={"X-Curie-Worker-Token": self._config.internal_worker_token},
                ) as response,
            ):
                status = response.status
                try:
                    payload: Any = await response.json(content_type=None)
                except (ValueError, aiohttp.ClientError):
                    payload = None
        except (aiohttp.ClientError, TimeoutError, OSError) as exc:
            logger.warning(
                "could not mint a channel read capability (%s): %s", mode, type(exc).__name__
            )
            return None
        if status == 404:
            if not getattr(self, "_channel_read_route_missing", False):
                self._channel_read_route_missing = True
                logger.info(
                    "the API has no channel read context route (404); turns run "
                    "without a channel read capability"
                )
            return None
        if status == 409:
            code = _refusal_code(payload)
            if code == _CHANNEL_READ_GRANT_ABSENT:
                raise ChannelReadGrantAbsent(code)
            logger.info("the API refused a channel read %s mint: %s", mode, code)
            return None
        if status != 200:
            logger.warning(
                "the API refused a channel read %s mint: HTTP %s %s",
                mode,
                status,
                _refusal_code(payload),
            )
            return None
        mint = _channel_read_mint(payload)
        if mint is None:
            logger.warning("the API answered a channel read %s mint with a malformed body", mode)
        return mint

    def fresh_boot_credential(self, env: Mapping[str, str]) -> dict[str, str]:
        """A copy of ``env`` whose boot state tokens carry a new credential id (#3823).

        Deleting a claim releases the credential its env carried, and a retry
        claims again from the same env. The substrate calls this for every new
        claim, so no claim boots with a credential an earlier claim released.
        Every other claim, the agent, scope, and expiry, is kept. A token
        without a credential id, or one this key did not sign, is left as is.
        """

        out = dict(env)
        api_key = self._config.api_key
        if not api_key:
            return out
        credential_id = uuid.uuid4().hex
        reissued: dict[str, str] = {}
        for key in (HISTORY_TOKEN_ENV, MEMORY_TOKEN_ENV, BootEnv.env_key("state_token")):
            token = out.get(key)
            if not token:
                continue
            if token not in reissued:
                fresh = _reissue_boot_token(token, api_key, credential_id)
                if fresh is None:
                    if boot_token_facts(token)[1] is not None:
                        logger.warning(
                            "could not give %s a new sandbox credential; it keeps its own", key
                        )
                    continue
                reissued[token] = fresh
            out[key] = reissued[token]
        return out


def _reissue_boot_token(token: str, api_key: str, credential_id: str) -> str | None:
    """``token`` re-signed with ``credential_id`` as its ``cred``, or None."""

    agent, cred, _exp = boot_token_facts(token)
    unsigned = _unsigned_payload(token)
    scope = None if unsigned is None else unsigned.get("scope")
    if agent is None or cred is None or not isinstance(scope, str):
        return None
    payload = sandbox_token.decode(token, api_key, agent=agent, scope=scope)
    if payload is None:
        return None
    claims = {k: v for k, v in payload.items() if k not in {"agent", "scope", "exp"}}
    claims["cred"] = credential_id
    return sandbox_token.mint(api_key, agent=agent, scope=scope, exp=payload["exp"], claims=claims)


def _unsigned_payload(token: str | None) -> dict[str, Any] | None:
    if not token or token.count(".") != 2:
        return None
    try:
        segment = token.split(".")[1]
        payload = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
    except (ValueError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def boot_token_facts(token: str | None) -> tuple[str | None, str | None, int | None]:
    """``(agent, cred, exp)`` from a boot state token this worker minted.

    An unsigned read of the payload. The signature is checked by the API.
    Returns three Nones when the token is absent or does not carry the
    #3823 credential id.
    """

    payload = _unsigned_payload(token)
    if payload is None:
        return None, None, None
    agent = payload.get("agent")
    cred = payload.get("cred")
    exp = payload.get("exp")
    if not isinstance(agent, str) or not isinstance(cred, str) or type(exp) is not int:
        return None, None, None
    return agent, cred, exp


def _boot_state_token_exp(token_ttl_s: float | None) -> int:
    """Absolute expiry of a boot-env state token (#3823).

    ``token_ttl_s`` is the turn's stream deadline. The token ends that many
    seconds later, plus ``BOOT_TOKEN_GRACE_SECONDS``, and never later than
    ``SANDBOX_TOKEN_TTL_SECONDS``. Omitted means the default execution
    deadline, so a caller that forgets the budget does not mint a day-long
    token.
    """

    if token_ttl_s is None:
        lifetime = DEFAULT_EXECUTION_DEADLINE_SECONDS
    else:
        lifetime = math.ceil(max(0.0, token_ttl_s))
    bounded = min(lifetime + BOOT_TOKEN_GRACE_SECONDS, SANDBOX_TOKEN_TTL_SECONDS)
    return int(time.time()) + bounded


def _refusal_code(payload: object) -> str | None:
    """The ``channel_read.*`` code of a refusal body, or None; never its message."""

    detail = payload.get("detail") if isinstance(payload, dict) else None
    code = detail.get("code") if isinstance(detail, dict) else None
    return code if isinstance(code, str) and _CHANNEL_READ_CODE.fullmatch(code) else None


def _channel_read_mint(payload: object) -> ChannelReadMint | None:
    if not isinstance(payload, dict):
        return None
    token = payload.get("token")
    generation = payload.get("generation")
    expires_at = payload.get("expires_at")
    turn_key = payload.get("turn_key")
    if (
        not isinstance(token, str)
        or not token
        or not isinstance(turn_key, str)
        or not turn_key
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation <= 0
        or isinstance(expires_at, bool)
        or not isinstance(expires_at, int)
    ):
        return None
    return ChannelReadMint(
        token=token, generation=generation, expires_at=expires_at, turn_key=turn_key
    )


def _binding_claim(kind: str | None, address: str | None) -> str | None:
    """The ADR-0188 ``binding`` claim: ``"<kind>:<address>"``, unquoted, the same
    string as the API's ``_binding_scope`` and
    ``workflow_state_entries.binding_scope``; None when the turn names no
    binding."""

    return f"{kind}:{address}" if kind and address else None


def apply_model_env(
    env: dict[str, str],
    config: WorkerConfig,
    model_override: str | None = None,
    reviewer_model_override: str | None = None,
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

    ``reviewer_model_override`` is the per-agent reviewer model (#4120).
    Without an override the runner resolves the default from its credential.

    The api_backend and env_key declarations
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
    if reviewer_model_override:
        env[REVIEWER_MODEL_ENV] = reviewer_model_override
    thinking = thinking_override if thinking_override is not None else config.thinking
    if thinking:
        env[THINKING_ENV] = thinking
    if config.false_completion_check:
        env[FALSE_COMPLETION_CHECK_ENV] = "1"


# ADR 0176 decision 2. The test cluster kubeconfig and the registry push and
# cache configs are connector secrets the sandbox must not receive. Frozen with
# the CLI and the chart in tests/vectors/e2e-connector-sandbox.json.
SANDBOX_WITHHELD_CONNECTOR_SECRETS = frozenset(
    {"E2E_CLUSTER_KUBECONFIG", "E2E_REGISTRY_PUSH_CONFIG", "E2E_BUILD_CACHE_CONFIG"}
)


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
        if name in SANDBOX_WITHHELD_CONNECTOR_SECRETS:
            # The test cluster kubeconfig stays in the hosted connector.
            # ADR 0176 decision 2. The marker must not name it either, or the
            # k8s substrate would look for a sandbox secretKeyRef.
            logger.warning(
                "Dropping connector secret withheld from the sandbox (never injected, never marked)"
            )
            continue
        if is_sealing_key_name(name):
            # @spec ACTION-EXECUTOR-16: the snapshot sealing key reaches only
            # the hosted connector. Withheld from every sandbox and kept out of
            # the marker, so the k8s substrate wires no secretKeyRef for it.
            logger.warning(
                "Dropping connector secret reserved for the sealing key "
                "(never injected, never marked)"
            )
            continue
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
