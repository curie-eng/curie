"""Sandbox substrate types: the claim handle, route record, config, and errors.

The substrate is the G1 seam between the worker kernel (F1) and the
kubernetes-sigs/agent-sandbox runtime. F1 talks in ``thread_key`` (the Slack
``thread_ts``, or any stable per-conversation key) and receives a
``SandboxHandle`` naming the claimed sandbox and its dial target. Everything
Kubernetes-shaped stays behind this module.
"""

from __future__ import annotations

import hashlib
import json
import socket
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, Protocol

from aci_protocol import BootEnv
from aci_protocol.service_config import API_KEY_ENV
from aci_protocol.slack_identities import SLACK_CREDENTIAL_ENV_PREFIXES
from curie_internal.keyspace import SANDBOX_KEY_PREFIX_DEFAULT
from plugin_format import is_reserved_boot_env_name

if TYPE_CHECKING:
    from ..repo_runner_pools import RepoRunnerPool

# Substrate-neutral labels: every backend tags its managed objects with these
# (the Kubernetes adapter on claims, the Docker adapter on containers), so they
# live here rather than in either concrete adapter.
MANAGED_BY_LABEL = "curietech.ai/managed-by"
# ADR 0100 (#2877): the bundle a Docker tier claim booted with, read back by
# ``get_claim`` (a Kubernetes claim carries it in its spec env).
BUNDLE_REF_LABEL = "curietech.ai/bundle-ref"
MANAGED_BY_VALUE = "curie-sandbox-substrate"
THREAD_HASH_LABEL = "curietech.ai/thread-hash"
# Claim-object label (not spec.additionalPodMetadata). The adopted controller
# rejects curietech.ai under additionalPodMetadata; template pod labels and
# claim metadata labels are the supported paths (#1488).
AGENT_LABEL = "curietech.ai/agent"
TURN_PROGRESS_ELIGIBILITY_ENV = "CURIE_TURN_PROGRESS_ENABLED"
# Thread key prefix of a connector action execution's sandbox (@spec
# ACTION-EXECUTOR-5): ``action-exec:<execution id>``. Pressure reclamation keys
# on it, so the substrate refuses a claim whose key and executor keyword disagree.
EXECUTOR_THREAD_KEY_PREFIX = "action-exec:"


def is_executor_thread_key(thread_key: str) -> bool:
    """Whether ``thread_key`` names a connector action execution's sandbox."""

    return thread_key.startswith(EXECUTOR_THREAD_KEY_PREFIX)


_GENERIC_POOL_SUFFIX = "-runner-pool"


def agent_warm_pool_name(base_pool: str, agent_name: str | None) -> str:
    """Derive the per-agent pool name matching charts/curie/templates/agent-sandbox.yaml.

    Generic pool: ``{fullname}-runner-pool``.
    Per-agent pool: ``{fullname}-agent-{agent}-runner-pool``.
    An operator override that does not use the chart suffix is left alone.
    """

    if not agent_name:
        return base_pool
    if not base_pool.endswith(_GENERIC_POOL_SUFFIX):
        return base_pool
    prefix = base_pool[: -len(_GENERIC_POOL_SUFFIX)]
    return f"{prefix}-agent-{agent_name}{_GENERIC_POOL_SUFFIX}"


def has_connector_secrets(env: Mapping[str, str] | None) -> bool:
    """Whether ``env`` carries a non-blank ``connector_secret_keys`` marker."""

    return bool((env or {}).get(BootEnv.env_key("connector_secret_keys"), "").strip())


def claim_warm_pool(
    base_pool: str,
    env: Mapping[str, str] | None,
    agent_name: str | None,
    agent_pools: frozenset[str],
    connector_secret_pools: frozenset[str],
) -> str:
    """Pick the warm pool a claim names.

    The chart states which per-agent pools it rendered: ``agent_pools`` is
    every one (CURIE_AGENT_SANDBOX_POOLS), ``connector_secret_pools`` those
    whose template carries connector secretKeyRefs
    (CURIE_AGENT_CONNECTOR_SECRET_POOLS). The ``connector_secret_keys`` marker
    comes from the agent's own bundle and does not agree with either: an agent
    deployed with ``curie cluster deploy`` can set it while no render produced
    its pool (#2943). A secret claim with no secret-capable pool raises
    MissingAgentPoolError at once instead of blocking on an object nothing
    created, and is never sent to another pool: connector secret values are
    stripped from the claim env and reach the pod only through that template.
    """

    if not agent_name:
        return base_pool
    pool = agent_warm_pool_name(base_pool, agent_name)
    if has_connector_secrets(env):
        # A non-chart base pool yields no derivable per-agent name.
        if agent_name in connector_secret_pools and pool != base_pool:
            return pool
        raise MissingAgentPoolError(agent_name, pool)
    if agent_name in agent_pools:
        return pool
    return base_pool


def repo_runner_entry(
    workspace_repo: str | None, pools: Mapping[str, RepoRunnerPool]
) -> tuple[str, RepoRunnerPool] | None:
    """The CURIE_REPO_RUNNER_POOLS entry bound to ``workspace_repo``, or None.

    GitHub names compare case-insensitively, as the kernel's repository fence
    does, so an exact key wins and a casefold match is the fallback.
    """

    if not workspace_repo or not pools:
        return None
    entry = pools.get(workspace_repo)
    if entry is not None:
        return workspace_repo, entry
    wanted = workspace_repo.casefold()
    for key, candidate in pools.items():
        if key.casefold() == wanted:
            return key, candidate
    return None


# The worker's own credentials, which never enter a sandbox. The connector
# caller signing key is here and the caller token it signs
# (``CURIE_CONNECTOR_CALLER_TOKEN``) is deliberately not: the token is this
# sandbox's own identity, short-lived and naming only its agent, and the runner
# must keep it to present to its hosted connectors (ADR-0168 decision 7). The
# key could mint a token naming any agent.
HOST_APPLICATION_CREDENTIAL_ENV_NAMES: frozenset[str] = frozenset(
    {
        "POSTGRES_PASSWORD",
        "DATABASE_URL",
        "VALKEY_PASSWORD",
        "SLACK_BOT_TOKEN",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        API_KEY_ENV,
        "LANGFUSE_SECRET_KEY",
        "CURIE_ADAPTER_CREDENTIALS",
        "CURIE_SEALING_PRIVATE_KEY",
        "CURIE_SEALING_PREVIOUS_PRIVATE_KEY",
        "CURIE_CONNECTOR_CALLER_SIGNING_KEY",
    }
)

#: A second Slack identity's tokens are indexed (ADR-0168 decision 1), so they
#: are matched by prefix rather than listed. Every name here is under ``CURIE_``,
#: which a connector secret may never declare, so no marker readmits one.
HOST_APPLICATION_CREDENTIAL_ENV_PREFIXES: tuple[str, ...] = SLACK_CREDENTIAL_ENV_PREFIXES


def _is_host_application_credential(name: str) -> bool:
    return name in HOST_APPLICATION_CREDENTIAL_ENV_NAMES or name.startswith(
        HOST_APPLICATION_CREDENTIAL_ENV_PREFIXES
    )


def filter_agent_child_env(env: Mapping[str, str] | None) -> dict[str, str]:
    """Return a copied child environment without host application credentials."""

    if env is None:
        return {}
    declared_connector_secret_names = {
        name for name in env.get(BootEnv.env_key("connector_secret_keys"), "").split(",") if name
    }
    return {
        name: value
        for name, value in env.items()
        if not _is_host_application_credential(name)
        or (name in declared_connector_secret_names and not is_reserved_boot_env_name(name))
    }


class RouteState(StrEnum):
    """Lifecycle state of a thread route recorded in the affinity store."""

    LIVE = "live"
    SUSPENDED = "suspended"


@dataclass(frozen=True)
class SandboxHandle:
    """A claimed sandbox bound to a thread: identity plus the ACI dial target.

    ``base_url`` is only resolvable from inside the cluster (the FQDN is a
    headless-Service DNS name); out-of-cluster callers (tests) reach the runner
    via a port-forward instead.
    """

    thread_key: str
    claim_name: str
    sandbox_name: str
    namespace: str
    service_fqdn: str
    port: int
    session_id: str
    history_ref: str | None = None
    # Per-claim bearer token the runner enforces on its ACI POST routes (issue
    # #63). Defaulted so RouteRecord.from_json rehydrates legacy Valkey records
    # (written before the token existed) with token == "" -- no header is sent
    # for those and the pre-token runner enforces nothing, so they keep working.
    token: str = ""
    # Worker-internal late-acquisition fence. These facts never cross ACI.
    workspace_repo: str | None = None
    # Exact commit extracted into /workspace for this runner. A verified PR
    # lineage can reuse the route only while this still matches its remote head.
    workspace_materialized_head: str | None = None
    # Highest publication result already present in durable transcript history
    # when this runner booted. Advancing it requires one cold rehydrate even
    # when a denied/failed revision leaves the git head unchanged.
    publication_visible_outcome_revision: int = 0
    generation: int = 0
    # CURIE_MAX_TURNS this runner booted with (#3071), None when absent. The
    # runner reads it once at boot, so a delivery wanting a different budget
    # must not adopt this route.
    max_turns: str | None = None
    # Whether this runner booted with a connector caller token (ADR-0168
    # decision 7). False for a record written before the field existed, which
    # is what makes the one replacement after an install gains a caller key.
    carries_caller_token: bool = False
    # The run id signed into the caller token at boot (ADR 0178), or None when
    # this runner was not booted for a work item. Missing on a route written
    # before the field existed, which reads as no run.
    caller_run: str | None = None
    # #3823: the boot state token's credential id, the agent it names, and its
    # absolute expiry. Missing on a route written before the field existed.
    # Release reports the id; a later turn replaces a route whose token has
    # already expired.
    state_credential_id: str | None = None
    state_credential_agent: str | None = None
    state_token_exp: int | None = None
    # Whether this runner booted with ADR 0130's model-visible tool and prompt.
    # False rehydrates legacy routes conservatively and forces one replacement
    # before an eligible turn may adopt them.
    carries_turn_progress: bool = False
    # The ``CURIE_BUNDLE_REF`` this runner booted with (ADR 0100), or None on a
    # route written before the field existed. Worker internal, never on ACI.
    bundle_ref: str | None = None
    # The repository runner image this runner booted from (ADR 0216): the
    # entry's index, amd64 and arm64 digests and its Docker flag. None and
    # False for an agent or generic pool, and on a route written before the
    # fields existed. A retained route whose pair no longer matches its
    # repository's entry is replaced, not reused.
    runner_digests: tuple[str, ...] | None = None
    runner_docker: bool = False

    @property
    def sandbox_id(self) -> str:
        """The stable sandbox identity (the Sandbox resource name)."""

        return self.sandbox_name

    @property
    def base_url(self) -> str:
        return f"http://{self.service_fqdn}:{self.port}"


@dataclass
class RouteRecord:
    """The affinity-store value for one thread: handle fields plus route state."""

    handle: SandboxHandle
    state: RouteState = RouteState.LIVE

    def to_json(self) -> str:
        payload = asdict(self.handle)
        payload["state"] = self.state.value
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> RouteRecord:
        payload = json.loads(raw)
        state = RouteState(payload.pop("state", RouteState.LIVE.value))
        # JSON has no tuple; equality with a freshly claimed handle needs one.
        if payload.get("runner_digests") is not None:
            payload["runner_digests"] = tuple(payload["runner_digests"])
        return cls(handle=SandboxHandle(**payload), state=state)


@dataclass(frozen=True)
class PressureCandidate:
    """One persisted live route eligible for bounded pressure inspection."""

    thread_key: str
    record: RouteRecord
    expires_at_ms: int


@dataclass(frozen=True)
class PressureScanResult:
    """A complete pressure inventory, or a fail closed scan outcome."""

    candidates: tuple[PressureCandidate, ...]
    outcome: Literal["complete", "scan-incomplete", "expiry-unsupported"]


@dataclass(frozen=True)
class SubstrateConfig:
    """Tunables for the substrate; defaults match the dev chart profile."""

    namespace: str
    warm_pool: str
    admission: AdmissionConfig | None = None
    # Agents the chart renders a per-agent pool for (agentSandbox.connectorSecrets,
    # agentSandbox.registryEgress, #3083, #2943, or agentSandbox.runnerImages,
    # ADR-0173), from CURIE_AGENT_SANDBOX_POOLS.
    agent_pools: frozenset[str] = frozenset()
    # The subset whose pool template carries connector secrets
    # (agentSandbox.connectorSecrets), from CURIE_AGENT_CONNECTOR_SECRET_POOLS.
    connector_secret_pools: frozenset[str] = frozenset()
    # Repository runner pools (ADR 0216), from CURIE_REPO_RUNNER_POOLS. A
    # repository with an entry claims its pool; one without routes as above.
    # Excluded from the hash: a dict is not hashable.
    repo_runner_pools: Mapping[str, RepoRunnerPool] = field(default_factory=dict, hash=False)
    # The digest pinned on CURIE_RUNNER_IMAGE, or None when it is a bare tag.
    # An entry's runner must have been built on exactly this image.
    platform_runner_digest: str | None = None
    runner_port: int = 8080
    # How long a live route stays bound with no touch. After expiry the claim
    # is an orphan and reap_orphans() deletes it.
    route_ttl_seconds: int = 3600
    # Suspended routes wait longer: the thread may come back tomorrow.
    suspended_route_ttl_seconds: int = 86400
    # How long claim() waits for a claim to bind a ready sandbox before raising
    # ClaimTimeoutError. This is a genuinely END-TO-END budget: a single shared
    # deadline in SandboxSubstrate._claim_fresh spans BOTH the bind phase and the
    # serviceFQDN phase, not one budget per phase, so the worst case is the
    # configured value once rather than twice. A cold create (pod scheduling +
    # bundle-fetch/extract init containers + runner boot + readiness) is ~30s on a
    # small node and can run longer under load, so the default is 90s. This is the
    # dominant term in the kernel's per-thread critical section: it MUST stay below
    # the lock TTL (WorkerConfig.lock_ttl_ms, 120s) so the lock never lapses
    # mid-claim and lets a second worker open a concurrent turn. Overridable via
    # CURIE_CLAIM_TIMEOUT_SECONDS; keep any override under the lock TTL too.
    claim_timeout_seconds: float = 90.0
    # The INITIAL bind/serviceFQDN poll interval, not the only one. A fixed 50ms
    # tick is cheap for a warm-pool bind that lands in a few hundred
    # milliseconds, and expensive for a cold boot that does not: an ~11s cold
    # create issues roughly 200 apiserver GETs per claim on Kubernetes, and on
    # the Docker substrate every tick is two docker forks plus a healthz call.
    # The interval therefore backs off after the fast window below.
    poll_interval_seconds: float = 0.05
    # Cap on any single sleep, so a claim that binds late is still noticed
    # promptly and the loop's overshoot past claim_timeout_seconds stays bounded
    # by one cap rather than growing with the backoff.
    poll_interval_max_seconds: float = 0.5
    # How many sleeps stay at poll_interval_seconds before backoff starts. Six
    # keeps every poll in the first 300ms byte-identical to the old fixed loop,
    # so a warm-pool claim that binds within 250ms today still binds on the same
    # poll: the request-volume win is taken entirely out of the cold-boot tail.
    poll_fast_polls: int = 6
    # Growth factor once the fast window is spent. Doubling reaches the cap in
    # four steps, so the tail costs tens of calls instead of hundreds.
    poll_backoff_factor: float = 2.0
    key_prefix: str = SANDBOX_KEY_PREFIX_DEFAULT
    claim_prefix: str = "curie-thread"
    # How long ``release(..., wait_gone=True)`` waits for the deleted claim AND
    # its sandbox/pod to disappear. Kubernetes delete of a SandboxClaim returns
    # while the object (and its pod) still exist; ResourceQuota charges the pod,
    # so a claim-only wait is not enough (#2259). Operator reset-thread keeps
    # the default False and is still bounded by the kernel's 5s release cap;
    # the eval plane waits because a following cluster message has only 45s.
    release_gone_timeout_seconds: float = 30.0

    def claim_name_for(self, thread_key: str, nonce: str) -> str:
        """A DNS-safe, per-generation claim name for a thread.

        The thread hash keeps names stable-per-thread for observability; the
        nonce distinguishes generations (a resume creates a new claim for the
        same thread).
        """

        digest = hashlib.sha256(thread_key.encode("utf-8")).hexdigest()[:10]
        return f"{self.claim_prefix}-{digest}-{nonce}"


@dataclass(frozen=True)
class QuotaRejection:
    """Observed ResourceQuota evidence from a rejected sandbox claim."""

    quota_name: str
    requested: Mapping[str, str]
    used: Mapping[str, str]
    hard: Mapping[str, str]


@dataclass(frozen=True)
class AdmissionConfig:
    """The namespace quota envelope published by the deployment."""

    quota_name: str
    priority_class: str
    ceiling: int
    hard: Mapping[str, str]


@dataclass(frozen=True)
class AdmissionRejection:
    """Computed admission evidence, independent of observed quota rejection."""

    quota_name: str
    requested: Mapping[str, str]
    used: Mapping[str, str]
    hard: Mapping[str, str]
    evidence_type: Literal["admission"] = "admission"


CapacityRejection = QuotaRejection | AdmissionRejection


@dataclass(frozen=True)
class QuotaStatus:
    name: str
    namespace: str
    spec_hard: Mapping[str, str]
    status_hard: Mapping[str, str]
    status_used: Mapping[str, str]


@dataclass(frozen=True)
class CapacityPod:
    claim_name: str | None
    resources: Mapping[str, str]


@dataclass(frozen=True)
class CapacityInventory:
    claims: frozenset[str]
    sandboxes: Mapping[str, str | None]
    pods: Mapping[str, CapacityPod]
    claim_resources: Mapping[str, Mapping[str, str]]
    suspended_claims: frozenset[str]
    observed_at: float


@dataclass(frozen=True)
class AdmissionReservation:
    state: Literal["preparing", "submitted", "bound", "retiring"]
    resources: Mapping[str, str]
    owner_token: str
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class ClaimView:
    """What the substrate needs to know about a SandboxClaim."""

    name: str
    ready: bool
    sandbox_name: str | None
    # The substrate-neutral instant the backing object was created, tz-aware
    # UTC. Its one reader is SandboxSubstrate.reap_orphans(), which spares a
    # claim still inside its bind window: such a claim has no route yet, so
    # "no live route names it" is ambiguous rather than proof of litter.
    #
    # ``None`` means the adapter cannot report an age. The reaper treats
    # unknown age as not-reapable (and warns), because wrongly reaping a live
    # claim kills a running sandbox while sparing an orphan only leaves litter.
    #
    # Required, with no default: a default would let an adapter silently omit
    # the field and silently disable the reaper's guard on that tier.
    created_at: datetime | None
    quota_rejection: QuotaRejection | None
    ready_reason: str | None
    ready_message: str | None
    # The ``CURIE_BUNDLE_REF`` the claim was created with, or None when the
    # adapter cannot report it (ADR 0100): the kernel uses it to recover which
    # deployment a retained sandbox runs.
    bundle_ref: str | None = None


@dataclass(frozen=True)
class SandboxView:
    """What the substrate needs to know about a Sandbox.

    ``port`` is the dial port when it is sandbox-specific (the Docker substrate
    publishes each runner on its own loopback host port); ``None`` means the
    substrate falls back to the fleet-wide ``SubstrateConfig.runner_port`` (the
    Kubernetes path, where every sandbox listens on the same in-cluster port).
    """

    name: str
    ready: bool
    service_fqdn: str | None
    operating_mode: str
    port: int | None = None


@dataclass(frozen=True)
class SandboxTermination:
    """Confirmed termination of the runner pod, with safe diagnostic text.

    ``exit_code`` is the runner container's terminated exit code when the pod
    status reported a real int; it never changes ``detail``.
    """

    reason: str
    detail: str | None = None
    exit_code: int | None = None


OperatingMode = Literal["Running", "Suspended"]

# The objects of one claim's Docker daemon (ADR 0217). The worker Role may
# create and delete daemon Secrets but never list or read them.
DaemonObjectKind = Literal["Pod", "Service", "NetworkPolicy", "Secret"]
ListableDaemonKind = Literal["Pod", "Service", "NetworkPolicy"]


@dataclass(frozen=True)
class DaemonObjectView:
    """What the substrate reads back about one daemon object.

    ``claim`` is its ``curietech.ai/sandbox-claim`` label; ``created_at`` is
    tz-aware, and None (unknown age) is never swept. ``phase`` is a Pod's;
    ``ready`` is a Pod's Ready condition, false once it is terminating.
    """

    kind: DaemonObjectKind
    name: str
    uid: str
    claim: str | None
    created_at: datetime | None
    phase: str | None = None
    ready: bool = False


@dataclass(frozen=True)
class ToolDaemonMount:
    """What the runner of a Docker-enabled claim receives (ADR 0217).

    ``files`` (ca.pem, cert.pem, key.pem) ride the per-claim token Secret and
    are mounted read-only; ``env`` holds the four non-secret DOCKER_* values.
    """

    files: Mapping[str, str] = field(repr=False)
    env: Mapping[str, str]
    active_deadline_seconds: int


class SandboxClient(Protocol):
    """What the substrate needs from the cluster, and nothing more.

    The port lives here in the substrate-neutral types module, NOT in a concrete
    adapter (#543): it is the seam every backend implements (Kubernetes, Docker),
    and declaring it inside the k8s adapter read as "the port is a Kubernetes
    thing" -- the opposite of the swap-readiness the seam exists to provide.
    """

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, object] | None = None,
        deadline: float,
        agent_name: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
        tool_daemon: ToolDaemonMount | None = None,
    ) -> None:
        """Create a claim after excluding host credentials from the child environment.

        Implementations must apply ``filter_agent_child_env`` before constructing
        any agent child environment.

        ``executor_secret_names`` is ``None`` for an ordinary claim. A set
        (possibly empty) marks an executor claim (@spec ACTION-EXECUTOR-5): the
        runner gets no model credential, no model env-key declaration, and no
        connector secret outside the set, whatever the pool template carries.

        ``deadline`` (``time.monotonic()`` based) bounds every request; one
        that has passed raises ClaimTimeoutError. ``tool_daemon`` hands the
        runner its daemon's client files and env. An HTTP 409 on a claim-name
        create raises ClaimNameConflict.
        """

        ...

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> ClaimView | None: ...

    def claim_resources(
        self,
        *,
        pool: str,
        runner_resources: dict[str, object] | None,
        agent_name: str | None,
        executor_secret_names: frozenset[str] | None,
    ) -> dict[str, str]: ...

    def capacity_inventory(
        self,
        *,
        priority_class: str,
        known_claim_resources: Mapping[str, Mapping[str, str]],
        request_timeout_seconds: float,
    ) -> CapacityInventory: ...

    def read_quota(
        self, quota_name: str, *, request_timeout_seconds: float
    ) -> QuotaStatus: ...

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None: ...

    def list_claims(self, *, label_selector: str) -> list[ClaimView]: ...

    def reap_claim_templates(self, *, keep: set[str], created_before: datetime) -> list[str]:
        """Delete claim-scoped templates whose claim is gone; return their names.

        A template is kept when its claim is in ``keep`` or it was created at or
        after ``created_before``, or its age is unknown. Substrates that create
        no claim-scoped objects return ``[]``.
        """

        ...

    def get_sandbox(self, name: str, *, request_timeout_seconds: float) -> SandboxView | None: ...

    def quota_has_headroom(
        self,
        rejection: QuotaRejection,
        *,
        request_timeout_seconds: float,
    ) -> bool: ...

    def pod_unschedulable(self, name: str, *, request_timeout_seconds: float) -> str | None:
        """The scheduler's message when pod ``name`` is ``PodScheduled=False``
        with reason ``Unschedulable``; None when it is scheduled, missing, or
        unreadable."""

        ...

    def pod_termination(
        self, name: str, *, since: datetime, request_timeout_seconds: float
    ) -> SandboxTermination | None:
        """Confirmed pod termination, or None when the evidence is absent."""

        ...

    def set_sandbox_mode(self, name: str, mode: OperatingMode) -> None: ...

    def create_daemon_object(
        self, namespace: str, manifest: Mapping[str, Any], *, request_timeout_seconds: float
    ) -> str:
        """Create one daemon object and return its uid; 409 is ClaimNameConflict."""

        ...

    def list_daemon_objects(
        self,
        namespace: str,
        kind: ListableDaemonKind,
        *,
        label_selector: str,
        request_timeout_seconds: float,
    ) -> list[DaemonObjectView]: ...

    def delete_daemon_object(
        self,
        namespace: str,
        kind: DaemonObjectKind,
        name: str,
        *,
        uid: str,
        request_timeout_seconds: float,
    ) -> None:
        """Delete exactly the object with ``uid``; absent or replaced is success."""

        ...

    def get_daemon_pod(
        self, namespace: str, name: str, *, request_timeout_seconds: float
    ) -> DaemonObjectView | None: ...

    def open_daemon_stream(
        self, namespace: str, pod: str, port: int, *, timeout_seconds: float
    ) -> AbstractContextManager[socket.socket]:
        """A raw byte stream to ``pod:port``; TLS is the caller's."""

        ...


class SandboxError(Exception):
    """Base error for the sandbox substrate."""


class ClaimNameConflict(SandboxError):
    """An object at a claim's name already exists (HTTP 409): it is someone else's."""


class ClaimTimeoutError(SandboxError):
    """The claim did not bind a ready sandbox within the configured timeout."""


class KubeTransientError(SandboxError):
    """A Kubernetes API read failed in transport or with a 5xx."""


class UnschedulableClaimError(ClaimTimeoutError):
    """The claim timed out while its pod was Unschedulable: no node has room.

    A ClaimTimeoutError subclass, so every caller that handles a claim timeout
    keeps doing so; only the factory work-item path treats it as capacity
    (#3169).
    """


class MissingAgentPoolError(SandboxError):
    """An agent declares connector secrets but the chart rendered no pool for it.

    Retrying cannot help: the pool appears only after an operator sets
    ``agentSandbox.connectorSecrets.<agent>`` and upgrades the release (#2943).
    """

    def __init__(self, agent_name: str, pool: str) -> None:
        self.agent_name = agent_name
        self.pool = pool
        super().__init__(
            f"agent {agent_name} declares connector secrets but SandboxWarmPool "
            f"{pool} was not rendered; set agentSandbox.connectorSecrets.{agent_name} "
            "in the release values and upgrade"
        )


class RepoRunnerPoolRefused(MissingAgentPoolError):
    """A repository's bound runner pool cannot serve this installation (ADR 0216).

    ``reason`` is ``missing_pool`` (the SandboxWarmPool was not rendered) or
    ``stale_base`` (the runner was built on another platform runner image, or
    this installation's runner image is unpinned). A subclass of
    MissingAgentPoolError so the kernel answers once and never retries:
    only a redeploy of the repository's runner fixes either.
    """

    def __init__(
        self,
        *,
        repo: str,
        pool: str,
        reason: Literal["missing_pool", "stale_base"],
        agent_name: str | None = None,
        base: str | None = None,
        platform_digest: str | None = None,
    ) -> None:
        self.agent_name = agent_name or ""
        self.pool = pool
        self.repo = repo
        self.reason = reason
        fix = f"`curie cluster factory --repo {repo}`"
        if reason == "missing_pool":
            message = (
                f"repository {repo} is bound to runner pool {pool}, but SandboxWarmPool "
                f"{pool} was not rendered (missing_pool); redeploy its runner with {fix}"
            )
        else:
            rebuild = (
                f"`curie cluster factory --repo-runner {repo}=<spec> --registry <registry> "
                "--namespace <namespace> --release <release> --context <context>`"
            )
            remedy = (
                "pin agentSandbox.runner.digest in this installation's release values first, "
                f"then rebuild and rebind it with {rebuild}"
                if platform_digest is None
                else f"rebuild and rebind it on the installed pinned platform runner with {rebuild}"
            )
            message = (
                f"repository {repo} runner image was built on platform runner {base}, "
                f"not this installation's {platform_digest or 'unpinned runner image'} "
                f"(stale_base); {remedy}; use a repository runner spec containing its retained "
                "tools, probes and Dockerfile declaration"
            )
        SandboxError.__init__(self, message)


class CapacityExhaustedError(SandboxError):
    """A sandbox claim was refused by admission or an observed quota limit."""

    def __init__(self, rejection: CapacityRejection) -> None:
        self.rejection = rejection
        source = (
            "sandbox admission" if isinstance(rejection, AdmissionRejection) else "ResourceQuota"
        )
        super().__init__(
            f"{source} {rejection.quota_name} rejected "
            f"requested={rejection.requested}; used={rejection.used}; "
            f"hard={rejection.hard}"
        )


class NoRouteError(SandboxError):
    """An operation needed an existing thread route and none was found."""


class RouteChangedError(SandboxError):
    """A fresh-only claim found (or raced) a live route it did not create.

    Attachment turns stage files into the runner created for them (#2739);
    adopting a runner another worker bound in the meantime would deliver the
    turn without its files, so the caller refuses instead.
    """


class SuspendedThreadError(SandboxError):
    """claim() was called on a suspended thread; the kernel must resume()
    explicitly so the stored history is carried into the replacement runner
    instead of silently forking a fresh, history-less session."""
