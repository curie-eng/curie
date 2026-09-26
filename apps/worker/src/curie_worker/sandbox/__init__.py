"""G1: the Agent Sandbox substrate (claim, affinity, suspend/resume, reap).

Public surface for F1 (the worker kernel):

- ``SandboxSubstrate`` -- claim/lookup/suspend/resume/release/reap_orphans
- ``AffinityStore`` -- the Valkey ``thread_ts -> sandbox_id`` route store
- ``KubernetesSandboxClient`` / ``SandboxClient`` -- the cluster seam
- ``SandboxHandle`` -- the claimed sandbox identity + ACI dial target
"""

from .affinity import AffinityStore
from .docker import DockerError, DockerSandboxClient, RunnerHardening
from .k8s import KubernetesSandboxClient
from .substrate import HISTORY_ENV, SESSION_ENV, SandboxSubstrate
from .types import (
    AGENT_LABEL,
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    THREAD_HASH_LABEL,
    CapacityExhaustedError,
    ClaimTimeoutError,
    ClaimView,
    NoRouteError,
    PressureCandidate,
    PressureScanResult,
    QuotaRejection,
    RouteChangedError,
    RouteRecord,
    RouteState,
    SandboxClient,
    SandboxError,
    SandboxHandle,
    SandboxView,
    SubstrateConfig,
    SuspendedThreadError,
    UnschedulableClaimError,
    agent_warm_pool_name,
    claim_warm_pool,
)

__all__ = [
    "AGENT_LABEL",
    "HISTORY_ENV",
    "MANAGED_BY_LABEL",
    "MANAGED_BY_VALUE",
    "SESSION_ENV",
    "THREAD_HASH_LABEL",
    "agent_warm_pool_name",
    "claim_warm_pool",
    "AffinityStore",
    "CapacityExhaustedError",
    "ClaimTimeoutError",
    "UnschedulableClaimError",
    "ClaimView",
    "DockerError",
    "DockerSandboxClient",
    "KubernetesSandboxClient",
    "NoRouteError",
    "PressureCandidate",
    "PressureScanResult",
    "QuotaRejection",
    "RouteChangedError",
    "RouteRecord",
    "RouteState",
    "RunnerHardening",
    "SandboxClient",
    "SandboxError",
    "SandboxHandle",
    "SandboxSubstrate",
    "SandboxView",
    "SubstrateConfig",
    "SuspendedThreadError",
]
