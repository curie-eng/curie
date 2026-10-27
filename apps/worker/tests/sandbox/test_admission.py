"""Shared admission runs before claim creation, even when quota usage is stale."""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any

import pytest
import redis
from curie_worker.run import _substrate_config
from curie_worker.sandbox import (
    AffinityStore,
    CapacityExhaustedError,
    SandboxHandle,
    SandboxSubstrate,
)

from .conftest import FakeSandboxClient

HARD = {
    "pods": "2",
    "requests.cpu": "500m",
    "requests.memory": "512Mi",
    "limits.cpu": "2",
    "limits.memory": "2Gi",
}
SMALL = {
    "pods": "1",
    "requests.cpu": "250m",
    "requests.memory": "256Mi",
    "limits.cpu": "1",
    "limits.memory": "1Gi",
}


def admission_env(*, ceiling: int = 2, hard: dict[str, str] | None = None) -> dict[str, str]:
    return {
        "CURIE_NAMESPACE": "test-ns",
        "CURIE_SANDBOX_ADMISSION": json.dumps(
            {
                "quota_name": "acme-sandbox-quota",
                "priority_class": "acme-sandbox",
                "ceiling": ceiling,
                "hard": HARD if hard is None else hard,
            }
        ),
    }


class AdmissionClient(FakeSandboxClient):
    """The external control plane can lag independently of the shared ledger."""

    def __init__(self, *, stale_inventory: bool = False) -> None:
        super().__init__()
        self.lock = threading.RLock()
        self.stale_inventory = stale_inventory
        self.started_at = time.time()
        self.inventory_error: Exception | None = None
        self.quota_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.keep_pods = False
        self.footprints: dict[str, dict[str, str]] = {}
        self.capacity_pods: dict[str, tuple[str | None, dict[str, str]]] = {}
        self.sandbox_owners: dict[str, str] = {}
        self.selected: dict[str, dict[str, str]] = {}
        self.hard = dict(HARD)
        self.create_entered = threading.Event()
        self.create_continue: threading.Event | None = None
        self.unknown_create = False

    def claim_resources(
        self,
        *,
        pool: str,
        runner_resources: dict[str, object] | None,
        agent_name: str | None,
        executor_secret_names: frozenset[str] | None,
    ) -> dict[str, str]:
        del pool, runner_resources, executor_secret_names
        return dict(self.selected.get(agent_name or "", SMALL))

    def capacity_inventory(self, *, priority_class: str, request_timeout_seconds: float) -> Any:
        from curie_worker.sandbox.types import CapacityInventory, CapacityPod

        assert priority_class == "acme-sandbox"
        assert 0 < request_timeout_seconds <= 1.0
        if self.inventory_error is not None:
            raise self.inventory_error
        with self.lock:
            return CapacityInventory(
                claims=frozenset() if self.stale_inventory else frozenset(self.claims),
                sandboxes={}
                if self.stale_inventory
                else {
                    sandbox.name: next(
                        (c.name for c in self.claims.values() if c.sandbox_name == sandbox.name),
                        self.sandbox_owners.get(sandbox.name),
                    )
                    for sandbox in self.sandboxes.values()
                },
                pods={}
                if self.stale_inventory
                else {
                    uid: CapacityPod(claim_name=owner, resources=resources)
                    for uid, (owner, resources) in self.capacity_pods.items()
                },
                claim_resources={} if self.stale_inventory else dict(self.footprints),
                suspended_claims=frozenset(
                    c.name
                    for c in self.claims.values()
                    if self.sandboxes.get(c.sandbox_name) is not None
                    and self.sandboxes[c.sandbox_name].operating_mode == "Suspended"
                ),
                observed_at=self.started_at if self.stale_inventory else time.time(),
            )

    def read_quota(self, quota_name: str, *, request_timeout_seconds: float) -> Any:
        from curie_worker.sandbox.types import QuotaStatus

        assert 0 < request_timeout_seconds <= 1.0
        if self.quota_error is not None:
            raise self.quota_error
        return QuotaStatus(
            name=quota_name,
            namespace=self.namespace,
            spec_hard=self.hard,
            status_hard=self.hard,
            status_used=dict.fromkeys(self.hard, "0"),
        )

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, object] | None = None,
        agent_name: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
    ) -> None:
        with self.lock:
            super().create_claim(
                name,
                pool=pool,
                env=env,
                labels=labels,
                runner_resources=runner_resources,
                agent_name=agent_name,
                executor_secret_names=executor_secret_names,
            )
            footprint = self.claim_resources(
                pool=pool,
                runner_resources=runner_resources,
                agent_name=agent_name,
                executor_secret_names=executor_secret_names,
            )
            self.footprints[name] = footprint
            self.capacity_pods[f"pod-{name}"] = (name, footprint)
            self.sandbox_owners[self.claims[name].sandbox_name] = name
            held = self.create_continue is not None and not self.create_entered.is_set()
            self.create_entered.set()
        if held:
            assert self.create_continue is not None
            assert self.create_continue.wait(5), "claim creator was not released"
        if self.unknown_create:
            raise TimeoutError("create result unknown")

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        with self.lock:
            super().delete_claim(name, request_timeout_seconds=request_timeout_seconds)
            if not self.keep_pods:
                self.capacity_pods = {
                    uid: pod for uid, pod in self.capacity_pods.items() if pod[0] != name
                }

    def set_sandbox_mode(self, name: str, mode: str) -> None:
        super().set_sandbox_mode(name, mode)
        if mode == "Suspended" and not self.keep_pods:
            owner = self.sandbox_owners[name]
            self.capacity_pods = {
                uid: pod for uid, pod in self.capacity_pods.items() if pod[0] != owner
            }


@pytest.fixture
def admission_client() -> AdmissionClient:
    return AdmissionClient()


def make_substrate(
    client: AdmissionClient,
    affinity: AffinityStore,
    key_prefix: str,
    *,
    enabled: bool = True,
    ceiling: int = 2,
    hard: dict[str, str] | None = None,
    namespace: str = "test-ns",
) -> SandboxSubstrate:
    env = admission_env(ceiling=ceiling, hard=hard) if enabled else {}
    config = replace(
        _substrate_config(env),
        namespace=namespace,
        warm_pool="test-pool",
        key_prefix=key_prefix,
        claim_timeout_seconds=0.1,
        poll_interval_seconds=0.001,
    )
    return SandboxSubstrate(client, affinity, config)


@pytest.mark.parametrize("store_count", [1, 4])
def test_concurrent_claims_reserve_before_credentials_or_create_with_stale_quota(
    redis_client: redis.Redis, affinity: AffinityStore, key_prefix: str, store_count: int
) -> None:
    client = AdmissionClient(stale_inventory=True)
    stores = [
        AffinityStore(
            redis_client,
            pressure_client=affinity._pressure_redis,  # noqa: SLF001
            key_prefix=key_prefix,
        )
        for _ in range(store_count)
    ]
    substrates = [make_substrate(client, store, key_prefix) for store in stores]
    minted: list[str] = []

    def mint(env: Any) -> dict[str, str]:
        minted.append(env["CONTENDER"])
        return dict(env)

    for substrate in substrates:
        substrate.set_boot_credential_minter(mint)
    start = threading.Barrier(8)

    def claim(index: int) -> SandboxHandle | CapacityExhaustedError:
        start.wait(timeout=5)
        try:
            return substrates[index % store_count].claim(
                f"contender-{index}", env={"CONTENDER": str(index)}
            )
        except CapacityExhaustedError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(claim, range(8)))

    assert len(client.created) <= 2, "stale quota usage allowed a third SandboxClaim"
    accepted = [result for result in results if isinstance(result, SandboxHandle)]
    assert len(accepted) == len(client.created) == len(minted) == 2
    assert len([r for r in results if isinstance(r, CapacityExhaustedError)]) == 6
    for index, result in enumerate(results):
        if isinstance(result, CapacityExhaustedError):
            assert affinity.get(f"contender-{index}") is None


def test_pending_unrouted_create_consumes_capacity(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str
) -> None:
    client = admission_client
    client.create_continue = threading.Event()
    substrate = make_substrate(client, affinity, key_prefix)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(substrate.claim, "pending")
        try:
            assert client.create_entered.wait(5)
            assert affinity.get("pending") is None
            substrate.claim("second")
            with pytest.raises(CapacityExhaustedError):
                substrate.claim("third")
            assert len(client.created) == 2
        finally:
            client.create_continue.set()
        assert pending.result(timeout=5).claim_name in client.claims


def test_unpublished_handoff_candidate_consumes_a_separate_reservation(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str
) -> None:
    substrate = make_substrate(admission_client, affinity, key_prefix)
    old = substrate.claim("retained")

    def validate(candidate: SandboxHandle) -> None:
        assert affinity.get("retained").handle == old  # type: ignore[union-attr]
        assert candidate.claim_name != old.claim_name
        with pytest.raises(CapacityExhaustedError):
            substrate.claim("while-unpublished")

    replacement = substrate.handoff(
        "retained", expected=old, env={}, workspace_repo=None, validate_candidate=validate
    )
    assert substrate.claim("retained") == replacement
    substrate.claim("after-retirement")
    assert len(admission_client.created) == 3


@pytest.mark.parametrize("path", ["resume", "executor"])
def test_resume_and_executor_cannot_bypass_full_admission(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str, path: str
) -> None:
    client = admission_client
    substrate = make_substrate(client, affinity, key_prefix)
    if path == "resume":
        handle = substrate.claim("resuming")
        substrate.suspend("resuming", history_ref="https://api.example.com/history/resuming")
        assert not any(owner == handle.claim_name for owner, _ in client.capacity_pods.values())
    substrate.claim("occupied-a")
    substrate.claim("occupied-b")
    created = list(client.created)
    with pytest.raises(CapacityExhaustedError):
        if path == "resume":
            substrate.resume("resuming", env={})
        else:
            substrate.claim("action-exec:acme-execution", executor_secret_names=frozenset())
    assert client.created == created


def test_deleting_owned_pod_holds_capacity_until_all_children_are_absent(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str
) -> None:
    client = admission_client
    substrate = make_substrate(client, affinity, key_prefix)
    victim = substrate.claim("victim")
    substrate.claim("retained")
    client.keep_pods = True
    assert substrate.release("victim")
    assert victim.claim_name not in client.claims
    assert victim.sandbox_name not in client.sandboxes
    with pytest.raises(CapacityExhaustedError):
        substrate.claim("waiting")
    assert len(client.created) == 2
    client.capacity_pods.pop(f"pod-{victim.claim_name}")
    substrate.claim("waiting")
    assert len(client.created) == 3


def test_external_clear_frees_ledger_without_rewriting_stale_route(
    admission_client: AdmissionClient,
    affinity: AffinityStore,
    key_prefix: str,
    redis_client: redis.Redis,
) -> None:
    client = admission_client
    substrate = make_substrate(client, affinity, key_prefix)
    victim = substrate.claim("externally-cleared")
    substrate.claim("retained")
    route_key = f"{key_prefix}:route:externally-cleared"
    original = redis_client.get(route_key)
    client.claims.pop(victim.claim_name)
    client.sandboxes.pop(victim.sandbox_name)
    client.capacity_pods.pop(f"pod-{victim.claim_name}")
    restarted = make_substrate(client, affinity, key_prefix)
    restarted.claim("after-clear")
    assert redis_client.get(route_key) == original
    assert len(client.created) == 3


def test_failed_delete_and_unknown_create_remain_charged(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str
) -> None:
    client = admission_client
    substrate = make_substrate(client, affinity, key_prefix)
    substrate.claim("retained")
    client.unknown_create = True
    client.delete_error = TimeoutError("deletion result unknown")
    with pytest.raises(TimeoutError, match="create result unknown"):
        substrate.claim("uncertain")
    client.unknown_create = False
    with pytest.raises(CapacityExhaustedError):
        make_substrate(client, affinity, key_prefix).claim("after-restart")
    assert len(client.created) == 2
    assert affinity.get("uncertain") is None


@pytest.mark.parametrize("failure", ["inventory", "quota"])
def test_unreadable_capacity_fails_closed_before_credentials(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str, failure: str
) -> None:
    client = admission_client
    setattr(client, f"{failure}_error", TimeoutError("capacity read unavailable"))
    substrate = make_substrate(client, affinity, key_prefix)
    minted: list[object] = []
    substrate.set_boot_credential_minter(lambda env: minted.append(env) or dict(env))
    with pytest.raises(TimeoutError, match="capacity read unavailable"):
        substrate.claim("unreadable", env={})
    assert client.created == minted == []
    setattr(client, f"{failure}_error", None)
    substrate.claim("readable-neighbor", env={})
    assert len(client.created) == len(minted) == 1


def test_enabled_zero_refuses_but_absent_contract_preserves_unrestricted_claims(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str
) -> None:
    zero = make_substrate(admission_client, affinity, key_prefix, ceiling=0)
    with pytest.raises(CapacityExhaustedError):
        zero.claim("enabled-zero")
    disabled = make_substrate(admission_client, affinity, key_prefix, enabled=False)
    for index in range(3):
        disabled.claim(f"disabled-{index}")
    assert len(admission_client.created) == 3


@pytest.mark.parametrize(
    "dimension", ["requests.cpu", "requests.memory", "limits.cpu", "limits.memory"]
)
def test_selected_larger_agent_footprint_limits_mixed_claims(
    admission_client: AdmissionClient, affinity: AffinityStore, key_prefix: str, dimension: str
) -> None:
    client = admission_client
    hard = {**HARD, "pods": "4"}
    client.hard = hard
    large = dict(SMALL)
    large[dimension] = HARD[dimension]
    client.selected["large-agent"] = large
    substrate = make_substrate(client, affinity, key_prefix, hard=hard, ceiling=4)
    large_handle = substrate.claim("large", agent_name="large-agent")
    assert substrate.claim("large", agent_name="large-agent") == large_handle
    with pytest.raises(CapacityExhaustedError):
        substrate.claim("small", agent_name="small-agent")
    assert len(client.created) == 1
    assert substrate.release("large")
    substrate.claim("small-a", agent_name="small-agent")
    substrate.claim("small-b", agent_name="small-agent")
    assert len(client.created) == 3
