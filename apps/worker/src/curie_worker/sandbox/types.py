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
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal, Protocol

from aci_protocol import BootEnv
from aci_protocol.service_config import API_KEY_ENV
from plugin_format import is_reserved_boot_env_name

# Substrate-neutral labels: every backend tags its managed objects with these
# (the Kubernetes adapter on claims, the Docker adapter on containers), so they
# live here rather than in either concrete adapter.
MANAGED_BY_LABEL = "curietech.ai/managed-by"
MANAGED_BY_VALUE = "curie-sandbox-substrate"
THREAD_HASH_LABEL = "curietech.ai/thread-hash"
# Claim-object label (not spec.additionalPodMetadata). The adopted controller
# rejects curietech.ai under additionalPodMetadata; template pod labels and
# claim metadata labels are the supported paths (#1488).
AGENT_LABEL = "curietech.ai/agent"

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


def claim_warm_pool(
    base_pool: str, env: Mapping[str, str] | None, agent_name: str | None
) -> str:
    """Route connector-secret claims to the per-agent pool, otherwise the generic pool."""

    marker = (env or {}).get(BootEnv.env_key("connector_secret_keys"), "").strip()
    if marker and agent_name:
        return agent_warm_pool_name(base_pool, agent_name)
    return base_pool

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
    }
)


def filter_agent_child_env(env: Mapping[str, str] | None) -> dict[str, str]:
    """Return a copied child environment without host application credentials."""

    if env is None:
        return {}
    declared_connector_secret_names = {
        name
        for name in env.get(BootEnv.env_key("connector_secret_keys"), "").split(",")
        if name
    }
    return {
        name: value
        for name, value in env.items()
        if name not in HOST_APPLICATION_CREDENTIAL_ENV_NAMES
        or (
            name in declared_connector_secret_names
            and not is_reserved_boot_env_name(name)
        )
    }


class RouteState(StrEnum):
    """Lifecycle state of a thread route recorded in the affinity store."""

    LIVE = "live"
    SUSPENDED = "suspended"


class AdoptionState(StrEnum):
    """Whether the route's runner credential has been installed on its pod.

    ADR-0122 delivers a warm bind's per-conversation credential over the first
    authenticated ACI ``Event`` and keeps the durable copy on the route, so a
    lost response, a crash, or a second replica recovers it from the store
    rather than from a worker's memory.

    - ``NONE``: cold path. The token reached the runner as per-claim boot env
      (``CURIE_RUNNER_TOKEN``); nothing is adoptable. Also what every route
      written before this field existed rehydrates as.
    - ``PENDING``: warm bind. The worker minted the credential and recorded it
      here BEFORE sending the adopting event; the runner still holds only the
      pool bootstrap, so the adopting request may be retried.
    - ``APPLIED``: the runner acked ``adoption_applied: true`` for this
      credential, or its ``/v1/status`` attestation proved the binding. The
      bootstrap is retired on that pod; only this credential reaches it.
    """

    NONE = "none"
    PENDING = "pending"
    APPLIED = "applied"


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
    # ADR-0122: whether ``token`` has been installed on the runner. Defaulted so
    # every route written before the field existed rehydrates as the cold path.
    adoption_state: AdoptionState = AdoptionState.NONE
    # ADR-0122 / root correction 3: the id of the event whose adopting turn
    # this route's pod was (or is being) bound with, recorded at the PENDING
    # write and cleared only once that turn's terminal answer is known. A
    # redelivery of that same event that finds the route APPLIED with its own
    # id here must not open a plain turn: the first turn may already have run.
    adopting_event_id: str | None = None

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
        # Mixed-fleet safety: a worker built before this field rehydrates a
        # record with ``SandboxHandle(**payload)`` and would raise on an
        # unknown key, then evict or reap the live claim. The cold path is the
        # default, so it is written in the legacy shape; only a route that is
        # actually pending or applied carries the key, and those are written
        # only once every replica understands it (rollout order: worker last).
        if payload["adoption_state"] == AdoptionState.NONE.value:
            del payload["adoption_state"]
        if payload["adopting_event_id"] is None:
            del payload["adopting_event_id"]
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> RouteRecord:
        payload = json.loads(raw)
        state = RouteState(payload.pop("state", RouteState.LIVE.value))
        # An unknown value raises rather than being guessed: a route that
        # claims a state this build does not know must not be treated as
        # either adoptable or applied.
        payload["adoption_state"] = AdoptionState(
            payload.get("adoption_state", AdoptionState.NONE.value)
        )
        return cls(handle=SandboxHandle(**payload), state=state)


@dataclass(frozen=True)
class SubstrateConfig:
    """Tunables for the substrate; defaults match the dev chart profile."""

    namespace: str
    warm_pool: str
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
    poll_interval_seconds: float = 0.05
    key_prefix: str = "curie:sandbox"
    claim_prefix: str = "curie-thread"

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
    resource: str
    requested: str
    used: str
    hard: str


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


OperatingMode = Literal["Running", "Suspended"]


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
    ) -> None:
        """Create a claim after excluding host credentials from the child environment.

        Implementations must apply ``filter_agent_child_env`` before constructing
        any agent child environment.
        """

        ...

    def get_claim(self, name: str) -> ClaimView | None: ...

    def delete_claim(self, name: str) -> None: ...

    def list_claims(self, *, label_selector: str) -> list[ClaimView]: ...

    def get_sandbox(self, name: str) -> SandboxView | None: ...

    def set_sandbox_mode(self, name: str, mode: OperatingMode) -> None: ...


class SandboxError(Exception):
    """Base error for the sandbox substrate."""


class ClaimTimeoutError(SandboxError):
    """The claim did not bind a ready sandbox within the configured timeout."""


class CapacityExhaustedError(SandboxError):
    """A sandbox claim was rejected by an observed ResourceQuota limit."""

    def __init__(self, rejection: QuotaRejection) -> None:
        self.rejection = rejection
        super().__init__(
            f"ResourceQuota {rejection.quota_name} rejected "
            f"{rejection.resource}={rejection.requested}; current usage is "
            f"{rejection.used}/{rejection.hard}"
        )


class NoRouteError(SandboxError):
    """An operation needed an existing thread route and none was found."""


class SuspendedThreadError(SandboxError):
    """claim() was called on a suspended thread; the kernel must resume()
    explicitly so the stored history is carried into the replacement runner
    instead of silently forking a fresh, history-less session."""
