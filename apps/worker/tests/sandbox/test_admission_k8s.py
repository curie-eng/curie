"""Admission reads actual Kubernetes ownership, Pod footprints and quota state."""

from __future__ import annotations

import copy
import json
import sys
import threading
import time as real_time
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from curie_worker.run import _substrate_config
from curie_worker.sandbox import AffinityStore, SandboxSubstrate
from curie_worker.sandbox import k8s as k8s_module
from curie_worker.sandbox import substrate as substrate_module
from curie_worker.sandbox.types import CapacityInventory, KubeTransientError
from kubernetes import client as kube
from kubernetes.utils.quantity import parse_quantity
from urllib3.exceptions import MaxRetryError, ProtocolError, ReadTimeoutError

from .test_k8s_claim import _chart_template_spec, _client, _FakeApi, _seed_chart

# importlib import mode does not add the tests directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from otel_fixtures import install  # noqa: E402


class InventoryAPI(_FakeApi):
    def __init__(self) -> None:
        super().__init__()
        self.pods: list[kube.V1Pod] = []
        self.pod_list_error: Exception | None = None
        self.pod_list_calls: list[float] = []
        self.runtime_reads: list[str] = []
        self.runtime_error: Exception | None = None
        self.overhead = {"cpu": "100m", "memory": "64Mi"}

    def list_namespaced_pod(self, namespace: str, **kwargs: Any) -> SimpleNamespace:
        assert namespace == "test-ns"
        assert 0 < kwargs["_request_timeout"] <= 1.0
        self.pod_list_calls.append(kwargs["_request_timeout"])
        if self.pod_list_error is not None:
            raise self.pod_list_error
        return SimpleNamespace(items=self.pods)

    def read_runtime_class(self, name: str, *, _request_timeout: float) -> SimpleNamespace:
        assert 0 < _request_timeout <= 1.0
        self.runtime_reads.append(name)
        if self.runtime_error is not None:
            raise self.runtime_error
        return SimpleNamespace(overhead=SimpleNamespace(pod_fixed=self.overhead))


def _resources(cpu: str, memory: str, limit_cpu: str, limit_memory: str) -> dict[str, Any]:
    return {
        "requests": {"cpu": cpu, "memory": memory, "ephemeral-storage": "1Gi"},
        "limits": {"cpu": limit_cpu, "memory": limit_memory, "ephemeral-storage": "2Gi"},
    }


def _footprint_client(api: InventoryAPI, *, runtime_class: bool = False) -> Any:
    spec = _chart_template_spec()
    pod = spec["podTemplate"]["spec"]
    if not runtime_class:
        pod.pop("runtimeClassName")
        pod["overhead"] = dict(api.overhead)
    pod["containers"][0]["resources"] = _resources("300m", "192Mi", "1", "512Mi")
    pod["containers"].append(
        {"name": "helper", "resources": _resources("200m", "64Mi", "500m", "256Mi")}
    )
    pod["initContainers"][0]["resources"] = _resources("800m", "512Mi", "1", "1Gi")
    pod["initContainers"][1]["resources"] = _resources("100m", "128Mi", "2", "512Mi")
    _seed_chart(api, spec=spec)
    client = _client(api)
    client._node_api = api
    return client


def _selected(client: Any, override: dict[str, Any] | None = None) -> dict[str, str]:
    return client.claim_resources(
        pool="curie-agent-acme-a-runner-pool",
        runner_resources=override,
        agent_name="acme-a",
        executor_secret_names=None,
    )


@pytest.mark.parametrize("runtime_class", [False, True])
def test_selected_footprint_uses_container_sum_init_peak_and_actual_overhead(
    runtime_class: bool,
) -> None:
    # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
    # https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/
    api = InventoryAPI()
    client = _footprint_client(api, runtime_class=runtime_class)
    original = copy.deepcopy(api.objects)
    actual = _selected(client)
    expected = {
        "pods": "1",
        "requests.cpu": "900m",
        "requests.memory": "576Mi",
        "limits.cpu": "2100m",
        "limits.memory": "1088Mi",
    }
    assert {key: parse_quantity(actual[key]) for key in expected} == {
        key: parse_quantity(value) for key, value in expected.items()
    }
    assert api.runtime_reads == (["gvisor"] if runtime_class else [])
    assert api.objects == original
    assert api.created == api.patches == []
    assert api.secrets == {}


def test_selected_footprint_applies_per_agent_override_before_any_write() -> None:
    api = InventoryAPI()
    client = _footprint_client(api)
    actual = _selected(client, _resources("600m", "512Mi", "2", "1Gi"))
    assert parse_quantity(actual["requests.cpu"]) == parse_quantity("1300m")
    assert parse_quantity(actual["requests.memory"]) == parse_quantity("1088Mi")
    assert parse_quantity(actual["limits.cpu"]) == parse_quantity("4100m")
    assert parse_quantity(actual["limits.memory"]) == parse_quantity("2112Mi")
    assert api.created == api.patches == []
    assert api.secrets == {}


def test_unreadable_configured_runtime_overhead_never_becomes_zero() -> None:
    api = InventoryAPI()
    client = _footprint_client(api, runtime_class=True)
    api.runtime_error = kube.ApiException(status=403)
    with pytest.raises(kube.ApiException):
        _selected(client)
    assert api.created == []


# The runtime-class and quota reads sit on the admission footprint path but
# outside _get, so they keep _get's transient classification themselves (#5406):
# 5xx statuses and transport errors -- including a MaxRetryError whose reason is
# not a timeout -- are KubeTransientError. A NewConnectionError would not do for
# the connection case: it inherits urllib3's TimeoutError, which
# _evaluate_admission already treats as transient.
#
# Every case here must flip red when the product change is reversed: this node
# is the issue's Fix pin, and the fix-pin verifier requires the whole node --
# every parametrized case -- to fail on the base code. The 403/404 preservation
# controls pass on the base by definition (the behavior is unchanged), so they
# live in their own sibling test below instead of in the pinned node.
_RUNTIME_QUOTA_TRANSIENT_FAULTS = [
    pytest.param(kube.ApiException(status=500), KubeTransientError, id="http-500"),
    pytest.param(kube.ApiException(status=502), KubeTransientError, id="http-502"),
    pytest.param(kube.ApiException(status=503), KubeTransientError, id="http-503"),
    pytest.param(kube.ApiException(status=504), KubeTransientError, id="http-504"),
    pytest.param(ProtocolError("connection reset by peer"), KubeTransientError, id="transport"),
    pytest.param(
        MaxRetryError(
            None,
            "/apis/node.k8s.io/v1/runtimeclasses/gvisor",
            ProtocolError("connection reset by peer"),
        ),
        KubeTransientError,
        id="sdk-retry",
    ),
]


@pytest.mark.parametrize("read", ["runtime-class", "quota"])
@pytest.mark.parametrize(("fault", "expected"), _RUNTIME_QUOTA_TRANSIENT_FAULTS)
def test_runtime_class_and_quota_reads_raise_kube_transient_error(
    read: str, fault: BaseException, expected: type[BaseException]
) -> None:
    api = InventoryAPI()
    client = _footprint_client(api, runtime_class=True)
    if read == "runtime-class":
        api.runtime_error = fault
        with pytest.raises(expected):
            _selected(client)
    else:
        api.quota_error = fault
        with pytest.raises(expected):
            client.read_quota("acme-sandbox-quota", request_timeout_seconds=1.0)
    assert api.created == api.patches == []


_RUNTIME_QUOTA_CLIENT_FAULTS = [
    pytest.param(kube.ApiException(status=403), id="http-403"),
    pytest.param(kube.ApiException(status=404), id="http-404"),
]


@pytest.mark.parametrize("read", ["runtime-class", "quota"])
@pytest.mark.parametrize("fault", _RUNTIME_QUOTA_CLIENT_FAULTS)
def test_runtime_class_and_quota_reads_keep_client_errors_as_api_exception(
    read: str, fault: BaseException
) -> None:
    api = InventoryAPI()
    client = _footprint_client(api, runtime_class=True)
    if read == "runtime-class":
        api.runtime_error = fault
        with pytest.raises(kube.ApiException):
            _selected(client)
    else:
        api.quota_error = fault
        with pytest.raises(kube.ApiException):
            client.read_quota("acme-sandbox-quota", request_timeout_seconds=1.0)
    assert api.created == api.patches == []


def _pod(
    name: str,
    *,
    owner_uid: str | None = None,
    owner_name: str = "owned-sandbox",
    phase: str = "Pending",
    deleting_seconds_ago: int | None = None,
    priority: str = "acme-sandbox",
) -> kube.V1Pod:
    owners = (
        [
            kube.V1OwnerReference(
                api_version="agents.x-k8s.io/v1alpha1",
                kind="Sandbox",
                name=owner_name,
                uid=owner_uid,
            )
        ]
        if owner_uid is not None
        else []
    )
    return kube.V1Pod(
        metadata=kube.V1ObjectMeta(
            name=name,
            uid=f"uid-{name}",
            owner_references=owners,
            deletion_timestamp=None
            if deleting_seconds_ago is None
            else datetime.now(UTC) - timedelta(seconds=deleting_seconds_ago),
            deletion_grace_period_seconds=30 if deleting_seconds_ago is not None else None,
        ),
        spec=kube.V1PodSpec(
            priority_class_name=priority,
            containers=[
                kube.V1Container(
                    name="runner",
                    resources=kube.V1ResourceRequirements(
                        requests={"cpu": "250m", "memory": "256Mi"},
                        limits={"cpu": "1", "memory": "1Gi"},
                    ),
                )
            ],
        ),
        status=kube.V1PodStatus(phase=phase),
    )


def test_inventory_follows_real_owner_chain_and_counts_unmatched_chargeable_pods() -> None:
    # Pinned controller ownership is Claim -> Sandbox -> Pod, without copied labels.
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/extensions/controllers/sandboxclaim_controller.go#L1232
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/controllers/sandbox_controller.go#L915
    api = InventoryAPI()
    spec = _chart_template_spec()
    spec["podTemplate"]["spec"].pop("runtimeClassName")
    _seed_chart(api, spec=spec)
    claim = api.seed(
        "sandboxclaims",
        "pending-claim",
        {"warmPoolRef": {"name": "curie-agent-acme-a-runner-pool"}},
    )
    sandbox = api.seed("sandboxes", "owned-sandbox", {"operatingMode": "Running"})
    sandbox["metadata"]["ownerReferences"] = [
        {"kind": "SandboxClaim", "name": "pending-claim", "uid": claim["metadata"]["uid"]}
    ]
    api.pods = [
        _pod("owned", owner_uid=sandbox["metadata"]["uid"]),
        _pod("replacement", owner_uid=sandbox["metadata"]["uid"]),
        _pod("warm"),
        _pod("terminating", deleting_seconds_ago=1),
        _pod("expired", deleting_seconds_ago=60),
        _pod("succeeded", phase="Succeeded"),
        _pod("failed", phase="Failed"),
        _pod("unrelated", priority="another-priority"),
    ]
    inventory = _client(api).capacity_inventory(
        priority_class="acme-sandbox", known_claim_resources={}, request_timeout_seconds=1
    )
    assert inventory.claims == frozenset({"pending-claim"})
    assert inventory.sandboxes["owned-sandbox"] == "pending-claim"
    assert set(inventory.pods) == {"uid-owned", "uid-replacement", "uid-warm", "uid-terminating"}
    assert inventory.pods["uid-owned"].claim_name == "pending-claim"
    assert inventory.pods["uid-warm"].claim_name is None
    del api.objects[("sandboxclaims", "pending-claim")]
    after = _client(api).capacity_inventory(
        priority_class="acme-sandbox", known_claim_resources={}, request_timeout_seconds=1
    )
    assert after.claims == frozenset()
    assert after.sandboxes["owned-sandbox"] == "pending-claim"
    assert after.pods["uid-owned"].claim_name == "pending-claim"


def _inventory_with_latency(
    api: InventoryAPI,
    monkeypatch: pytest.MonkeyPatch,
    *,
    known_claim_resources: Mapping[str, Mapping[str, str]],
) -> tuple[CapacityInventory, list[str], float]:
    elapsed = 0.0
    reads: list[str] = []

    def with_latency(method: Callable[..., Any]) -> Callable[..., Any]:
        def read(*args: Any, **kwargs: Any) -> Any:
            nonlocal elapsed
            assert 0 < kwargs["_request_timeout"] <= 1.0
            elapsed += 0.015
            reads.append(method.__name__)
            return method(*args, **kwargs)

        return read

    for name in (
        "list_namespaced_custom_object",
        "get_namespaced_custom_object",
        "list_namespaced_pod",
        "read_runtime_class",
    ):
        monkeypatch.setattr(api, name, with_latency(getattr(api, name)))
    monkeypatch.setattr(k8s_module.time, "monotonic", lambda: elapsed)
    client = _client(api)
    client._node_api = api
    inventory = client.capacity_inventory(
        priority_class="acme-sandbox",
        known_claim_resources=known_claim_resources,
        request_timeout_seconds=1,
    )
    return inventory, reads, elapsed


def test_inventory_of_fifty_bound_claims_fits_one_second_with_constant_api_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/extensions/controllers/sandboxclaim_controller.go#L1232
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/controllers/sandbox_controller.go#L915
    # https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/
    api = InventoryAPI()
    expected_resources: dict[str, dict[str, str]] = {}
    expected_owners: dict[str, str] = {}
    expected_pod_owners: dict[str, str] = {}
    for index in range(50):
        claim_name = f"claim-{index}"
        sandbox_name = f"sandbox-{index}"
        pool_name = f"{claim_name}-resources-pool"
        template_name = f"{claim_name}-resources"
        spec = _chart_template_spec()
        pod_spec = spec["podTemplate"]["spec"]
        pod_spec["priorityClassName"] = "acme-sandbox"
        resources = _resources(f"{250 + index}m", f"{256 + index}Mi", "1", "1Gi")
        pod_spec["containers"][0]["resources"] = resources
        api.seed("sandboxtemplates", template_name, spec)
        api.seed(
            "sandboxwarmpools",
            pool_name,
            {"replicas": 0, "sandboxTemplateRef": {"name": template_name}},
        )
        claim = api.seed("sandboxclaims", claim_name, {"warmPoolRef": {"name": pool_name}})
        claim["status"] = {"sandbox": {"name": sandbox_name}}
        sandbox = api.seed("sandboxes", sandbox_name, {**spec, "operatingMode": "Running"})
        sandbox["metadata"]["ownerReferences"] = [
            {
                "apiVersion": "extensions.agents.x-k8s.io/v1beta1",
                "kind": "SandboxClaim",
                "name": claim_name,
                "uid": claim["metadata"]["uid"],
            }
        ]
        pod = _pod(
            f"pod-{index}",
            owner_uid=sandbox["metadata"]["uid"],
            owner_name=sandbox_name,
            phase="Running",
        )
        pod.spec.containers[0].resources = kube.V1ResourceRequirements(**resources)
        pod.spec.runtime_class_name = "gvisor"
        pod.spec.overhead = dict(api.overhead)
        api.pods.append(pod)
        expected_resources[claim_name] = {
            "pods": "1",
            "requests.cpu": f"{350 + index}m",
            "requests.memory": f"{320 + index}Mi",
            "limits.cpu": "1100m",
            "limits.memory": "1088Mi",
        }
        expected_owners[sandbox_name] = claim_name
        expected_pod_owners[f"uid-pod-{index}"] = claim_name

    inventory, reads, elapsed = _inventory_with_latency(
        api, monkeypatch, known_claim_resources={}
    )

    assert inventory.claims == frozenset(expected_resources)
    assert inventory.sandboxes == expected_owners
    assert {
        uid: pod.claim_name for uid, pod in inventory.pods.items()
    } == expected_pod_owners
    assert {
        name: {key: parse_quantity(value) for key, value in values.items()}
        for name, values in inventory.claim_resources.items()
    } == {
        name: {key: parse_quantity(value) for key, value in values.items()}
        for name, values in expected_resources.items()
    }
    for uid, pod in inventory.pods.items():
        assert {key: parse_quantity(value) for key, value in pod.resources.items()} == {
            key: parse_quantity(value)
            for key, value in expected_resources[expected_pod_owners[uid]].items()
        }
    assert inventory.suspended_claims == frozenset()
    assert len(reads) <= 4
    assert len(api.runtime_reads) <= 1
    assert elapsed < 1.0


def test_inventory_of_fifty_pending_claims_uses_known_resources_with_three_lists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = InventoryAPI()
    known_resources = {
        f"pending-{index}": {
            "pods": "1",
            "requests.cpu": "0.5",
            "requests.memory": str((256 + index) * 1024**2),
            "limits.cpu": "1",
            "limits.memory": str(1024**3),
        }
        for index in range(50)
    }
    for name in known_resources:
        api.seed(
            "sandboxclaims", name, {"warmPoolRef": {"name": f"{name}-resources-pool"}}
        )

    inventory, reads, elapsed = _inventory_with_latency(
        api, monkeypatch, known_claim_resources=known_resources
    )

    assert inventory.claims == frozenset(known_resources)
    assert inventory.claim_resources == known_resources
    assert inventory.sandboxes == inventory.pods == {}
    assert inventory.suspended_claims == frozenset()
    assert reads.count("list_namespaced_custom_object") == 2
    assert reads.count("list_namespaced_pod") == 1
    assert len(reads) == 3
    assert api.runtime_reads == []
    assert elapsed < 1.0


def test_inventory_uses_owned_sandbox_spec_when_old_pool_is_missing() -> None:
    # https://github.com/kubernetes-sigs/agent-sandbox/blob/v0.5.0/extensions/controllers/sandboxclaim_controller.go#L1232
    api = InventoryAPI()
    client = _footprint_client(api, runtime_class=True)
    pool_name = "curie-agent-acme-a-runner-pool"
    spec = api.objects[("sandboxtemplates", "curie-agent-acme-a-runner")]["spec"]
    del api.objects[("sandboxwarmpools", pool_name)]
    claim = api.seed("sandboxclaims", "bound-claim", {"warmPoolRef": {"name": pool_name}})
    claim["status"] = {"sandbox": {"name": "owned-sandbox"}}
    sandbox = api.seed("sandboxes", "owned-sandbox", {**spec, "operatingMode": "Running"})
    sandbox["metadata"]["ownerReferences"] = [
        {"kind": "SandboxClaim", "name": "bound-claim", "uid": claim["metadata"]["uid"]}
    ]

    inventory = client.capacity_inventory(
        priority_class="acme-sandbox", known_claim_resources={}, request_timeout_seconds=1
    )

    assert inventory.claims == frozenset({"bound-claim"})
    assert inventory.sandboxes == {"owned-sandbox": "bound-claim"}
    assert inventory.pods == {}
    assert inventory.suspended_claims == frozenset()
    expected = {
        "pods": "1",
        "requests.cpu": "900m",
        "requests.memory": "576Mi",
        "limits.cpu": "2100m",
        "limits.memory": "1088Mi",
    }
    assert set(inventory.claim_resources) == {"bound-claim"}
    assert {
        key: parse_quantity(value)
        for key, value in inventory.claim_resources["bound-claim"].items()
    } == {key: parse_quantity(value) for key, value in expected.items()}
    assert api.calls == []
    assert api.runtime_reads == ["gvisor"]


@pytest.mark.parametrize("resource", ["sandboxclaims", "sandboxes", "pods"])
def test_incomplete_inventory_raises_instead_of_reporting_empty_capacity(resource: str) -> None:
    api = InventoryAPI()
    if resource == "pods":
        api.pod_list_error = kube.ApiException(status=403)
    else:
        api.failures[("list", resource)] = kube.ApiException(status=403)
    with pytest.raises(kube.ApiException):
        _client(api).capacity_inventory(
            priority_class="acme-sandbox", known_claim_resources={}, request_timeout_seconds=1
        )


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_pod_list_transient_http_status_preserves_cause_without_retry(status: int) -> None:
    api = InventoryAPI()
    # #5544 observed the Kubernetes SDK raising ApiException(status=503) on Pod LIST.
    fault = kube.ApiException(status=status)
    api.pod_list_error = fault
    with pytest.raises(KubeTransientError) as error:
        _client(api).capacity_inventory(
            priority_class="acme-sandbox", known_claim_resources={}, request_timeout_seconds=1
        )
    assert error.value.__cause__ is fault
    assert len(api.pod_list_calls) == 1
    # The three LISTs are concurrent (#5630), so only their set is fixed.
    assert sorted(plural for plural, _selector in api.lists) == ["sandboxclaims", "sandboxes"]
    assert api.created == api.patches == []
    assert api.secrets == {}


@pytest.mark.parametrize("status", [403, 404])
def test_pod_list_permanent_http_status_preserves_api_exception(status: int) -> None:
    api = InventoryAPI()
    fault = kube.ApiException(status=status)
    api.pod_list_error = fault
    with pytest.raises(kube.ApiException) as error:
        _client(api).capacity_inventory(
            priority_class="acme-sandbox", known_claim_resources={}, request_timeout_seconds=1
        )
    assert error.value is fault
    assert len(api.pod_list_calls) == 1
    assert api.created == api.patches == []
    assert api.secrets == {}


@pytest.mark.parametrize("budget", [0.0, -0.01, 0.01])
def test_inventory_exhausted_budget_stops_before_next_kubernetes_request(
    monkeypatch: pytest.MonkeyPatch,
    budget: float,
) -> None:
    api = InventoryAPI()
    # A claim with no known footprint needs its pool's template read once the
    # LISTs have answered: the next Kubernetes request after them.
    api.seed(
        "sandboxclaims",
        "pending-claim",
        {"warmPoolRef": {"name": "curie-agent-acme-a-runner-pool"}},
    )
    # Every LIST answers exactly as the whole budget runs out.
    issued = _slow_inventory_lists(
        api,
        monkeypatch,
        _SimulatedClock(),
        dict.fromkeys(_INVENTORY_LISTS, 0.01),
        barrier=threading.Barrier(3, timeout=5),
    )
    with pytest.raises(TimeoutError, match="capacity inventory deadline expired"):
        _client(api).capacity_inventory(
            priority_class="acme-sandbox",
            known_claim_resources={},
            request_timeout_seconds=budget,
        )
    if budget > 0:
        # Each LIST issued once, concurrently, with the budget left at issue.
        assert sorted(issued) == [(resource, budget) for resource in sorted(_INVENTORY_LISTS)]
        assert sorted(plural for plural, _selector in api.lists) == [
            "sandboxclaims",
            "sandboxes",
        ]
    else:
        assert issued == []
        assert api.lists == []
        assert api.pod_list_calls == []
    assert [call for call in api.calls if call[0] == "get"] == []
    assert api.created == api.patches == []
    assert api.secrets == {}


def test_short_inventory_budget_preserves_sdk_timeout_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = InventoryAPI()
    # #5296 observed MaxRetryError caused by ReadTimeoutError on Pod LIST.
    cause = ReadTimeoutError(None, "/api/v1/namespaces/test-ns/pods", "read timed out")
    api.pod_list_error = MaxRetryError(None, cause.url, cause)
    # A simulated clock keeps the short budget from expiring on a slow host
    # before the concurrent LISTs are issued.
    issued = _slow_inventory_lists(
        api, monkeypatch, _SimulatedClock(), dict.fromkeys(_INVENTORY_LISTS, 0.0)
    )
    with pytest.raises(MaxRetryError) as error:
        _client(api).capacity_inventory(
            priority_class="acme-sandbox",
            known_claim_resources={},
            request_timeout_seconds=0.086,
        )
    assert error.value.reason is cause
    # Each LIST issued once (no retry), none with more than the budget.
    assert sorted(resource for resource, _timeout in issued) == sorted(_INVENTORY_LISTS)
    assert all(0 < timeout <= 0.086 for _resource, timeout in issued)
    assert sorted(plural for plural, _selector in api.lists) == ["sandboxclaims", "sandboxes"]
    assert len(api.pod_list_calls) == 1
    assert api.created == api.patches == []


# -- #5630: the three inventory LISTs share one budget concurrently -----------

_INVENTORY_LISTS = ("sandboxclaims", "sandboxes", "pods")


class _SimulatedClock:
    """Monotonic time that only the fake Kubernetes API advances.

    A fake LIST reads its issue instant, then moves the clock to the instant its
    own response arrives; ``advance_to`` keeps the latest arrival, so LISTs in
    flight together cost the slowest of them, not their sum. Thread-safe, because
    the client issues the LISTs from worker threads.
    """

    def __init__(self) -> None:
        self._now = 0.0
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self._now

    def advance_to(self, instant: float) -> None:
        with self._lock:
            self._now = max(self._now, instant)


def _slow_inventory_lists(
    api: InventoryAPI,
    monkeypatch: pytest.MonkeyPatch,
    clock: _SimulatedClock,
    latency: Mapping[str, float],
    *,
    barrier: threading.Barrier | None = None,
) -> list[tuple[str, float]]:
    """Give each inventory LIST a fixed server latency on ``clock``.

    Only LISTs that carry ``_request_timeout`` are inventory reads; the reaper's
    own claim and template LISTs pass straight through. A LIST whose latency
    exceeds the timeout it was handed raises urllib3's ``ReadTimeoutError`` once
    that timeout elapses, as a real server's client would; otherwise it answers
    after its latency. ``barrier`` holds each LIST until all three are in
    flight, which a sequential caller never achieves. Returns the
    ``(resource, timeout)`` of every inventory LIST issued.
    """

    issued: list[tuple[str, float]] = []
    issued_lock = threading.Lock()

    def timed(resource_of: Callable[[tuple[Any, ...]], str], inner: Callable[..., Any]) -> Any:
        def call(*args: Any, **kwargs: Any) -> Any:
            timeout = kwargs.get("_request_timeout")
            if timeout is None:
                return inner(*args, **kwargs)
            resource = resource_of(args)
            issued_at = clock.monotonic()
            with issued_lock:
                issued.append((resource, float(timeout)))
            if barrier is not None:
                barrier.wait()
            cost = latency[resource]
            if cost > timeout:
                clock.advance_to(issued_at + timeout)
                raise ReadTimeoutError(None, resource, f"read timeout={timeout}")
            clock.advance_to(issued_at + cost)
            return inner(*args, **kwargs)

        return call

    monkeypatch.setattr(
        api,
        "list_namespaced_custom_object",
        timed(lambda args: str(args[3]), api.list_namespaced_custom_object),
    )
    monkeypatch.setattr(
        api, "list_namespaced_pod", timed(lambda _args: "pods", api.list_namespaced_pod)
    )
    # Patch only the module's view of ``time``, so the executor and asyncio keep
    # the real clock.
    monkeypatch.setattr(
        k8s_module, "time", SimpleNamespace(monotonic=clock.monotonic, time=real_time.time)
    )
    return issued


def _seed_owned_inventory(api: InventoryAPI) -> None:
    """One claim owning a sandbox owning a Pod, plus one unowned warm Pod."""

    claim = api.seed(
        "sandboxclaims",
        "pending-claim",
        {"warmPoolRef": {"name": "curie-agent-acme-a-runner-pool"}},
    )
    sandbox = api.seed("sandboxes", "owned-sandbox", {"operatingMode": "Running"})
    sandbox["metadata"]["ownerReferences"] = [
        {"kind": "SandboxClaim", "name": "pending-claim", "uid": claim["metadata"]["uid"]}
    ]
    api.pods = [_pod("owned", owner_uid=sandbox["metadata"]["uid"]), _pod("warm")]


@pytest.mark.parametrize(
    "slow_list",
    [None, "sandboxclaims", "sandboxes", "pods"],
    ids=["healthy", "slow-claims", "slow-sandboxes", "slow-pods"],
)
def test_inventory_lists_overlap_within_one_second_budget(
    monkeypatch: pytest.MonkeyPatch, slow_list: str | None
) -> None:
    # Three healthy 0.45 s LISTs sum to 1.35 s, so issued one after another they
    # exhaust the 1.0 s total (#5630); in flight together they fit inside it.
    api = InventoryAPI()
    _seed_owned_inventory(api)
    latency = dict.fromkeys(_INVENTORY_LISTS, 0.45)
    if slow_list is not None:
        # The control: one LIST slower than the whole bound still fails closed.
        latency[slow_list] = 1.1
    clock = _SimulatedClock()
    issued = _slow_inventory_lists(
        api, monkeypatch, clock, latency, barrier=threading.Barrier(3, timeout=5)
    )
    client = _client(api)

    def inventory() -> CapacityInventory:
        return client.capacity_inventory(
            priority_class="acme-sandbox", known_claim_resources={}, request_timeout_seconds=1.0
        )

    if slow_list is None:
        result = inventory()
        assert result.claims == frozenset({"pending-claim"})
        assert result.sandboxes == {"owned-sandbox": "pending-claim"}
        assert set(result.pods) == {"uid-owned", "uid-warm"}
        assert result.pods["uid-owned"].claim_name == "pending-claim"
        assert result.pods["uid-warm"].claim_name is None
        assert set(result.claim_resources) == {"pending-claim"}
        assert clock.monotonic() == pytest.approx(0.45)
    else:
        with pytest.raises(ReadTimeoutError):
            inventory()
        # Bounded by the 1.0 s total, not by the slow LIST.
        assert clock.monotonic() == pytest.approx(1.0)
    # Each LIST issued exactly once (no retry), with a positive timeout no
    # larger than the one-second bound.
    assert sorted(resource for resource, _timeout in issued) == sorted(_INVENTORY_LISTS)
    assert all(0 < timeout <= 1.0 for _resource, timeout in issued)
    assert api.created == api.patches == []


def test_reap_with_slow_healthy_lists_reconciles_admission(
    monkeypatch: pytest.MonkeyPatch, affinity: AffinityStore, key_prefix: str
) -> None:
    api = InventoryAPI()
    client = _footprint_client(api)
    pool = "curie-agent-acme-a-runner-pool"
    footprint = _selected(client)
    hard = {key: str(parse_quantity(value) * 4) for key, value in footprint.items()}
    api.quota = SimpleNamespace(
        metadata=SimpleNamespace(name="acme-sandbox-quota", namespace="test-ns"),
        spec=SimpleNamespace(hard=hard),
        status=SimpleNamespace(hard=hard, used=dict.fromkeys(hard, "0")),
    )
    # A live claim with no ledger row: its footprint needs the template read
    # that follows the LISTs on the sequential remainder of the budget.
    api.seed("sandboxclaims", "pending-claim", {"warmPoolRef": {"name": pool}})
    admission = {
        "quota_name": "acme-sandbox-quota",
        "priority_class": "acme-sandbox",
        "ceiling": 4,
        "hard": hard,
    }
    config = replace(
        _substrate_config(
            {"CURIE_NAMESPACE": "test-ns", "CURIE_SANDBOX_ADMISSION": json.dumps(admission)}
        ),
        namespace="test-ns",
        key_prefix=key_prefix,
    )
    substrate = SandboxSubstrate(client, affinity, config)

    # The client seam records the bound each caller hands the inventory.
    phase = ["maintenance"]
    bounds: list[tuple[str, float]] = []
    real_inventory = client.capacity_inventory

    def recording_inventory(**kwargs: Any) -> CapacityInventory:
        bounds.append((phase[0], kwargs["request_timeout_seconds"]))
        return real_inventory(**kwargs)

    monkeypatch.setattr(client, "capacity_inventory", recording_inventory)
    issued = _slow_inventory_lists(
        api,
        monkeypatch,
        _SimulatedClock(),
        dict.fromkeys(_INVENTORY_LISTS, 0.45),
        barrier=threading.Barrier(3, timeout=5),
    )
    probe = install(monkeypatch, substrate_module)

    assert substrate.reap_orphans() == []

    cleanup = [span for span in probe.spans if span.name == "curie.sandbox.cleanup"]
    assert len(cleanup) == 1
    assert cleanup[0].events == [("sandbox.cleanup.completed", {"outcome": "observed"})]
    assert [point.attributes["outcome"] for point in probe.points("curie.sandbox.cleanup")] == [
        "observed"
    ]

    phase[0] = "turn-admission"
    assert (
        substrate._evaluate_admission(
            "admission-probe",
            operation="inspect",
            pool=pool,
            runner_resources=None,
            agent_name="acme-a",
            executor_secret_names=None,
        )
        is None
    )

    assert [caller for caller, _bound in bounds] == ["maintenance", "turn-admission"]
    assert all(0 < bound <= 1.0 for _caller, bound in bounds)
    assert sorted(resource for resource, _timeout in issued) == sorted(_INVENTORY_LISTS * 2)
    assert all(0 < timeout <= 1.0 for _resource, timeout in issued)
