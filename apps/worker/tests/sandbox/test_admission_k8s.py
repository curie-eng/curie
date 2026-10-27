"""Admission reads actual Kubernetes ownership, Pod footprints and quota state."""

from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from kubernetes import client as kube
from kubernetes.utils.quantity import parse_quantity

from .test_k8s_claim import _chart_template_spec, _client, _FakeApi, _seed_chart


class InventoryAPI(_FakeApi):
    def __init__(self) -> None:
        super().__init__()
        self.pods: list[kube.V1Pod] = []
        self.pod_list_error: Exception | None = None
        self.runtime_reads: list[str] = []
        self.runtime_error: Exception | None = None
        self.overhead = {"cpu": "100m", "memory": "64Mi"}

    def list_namespaced_pod(self, namespace: str, **kwargs: Any) -> SimpleNamespace:
        assert namespace == "test-ns"
        assert 0 < kwargs["_request_timeout"] <= 1.0
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
        priority_class="acme-sandbox", request_timeout_seconds=1
    )
    assert inventory.claims == frozenset({"pending-claim"})
    assert inventory.sandboxes["owned-sandbox"] == "pending-claim"
    assert set(inventory.pods) == {"uid-owned", "uid-replacement", "uid-warm", "uid-terminating"}
    assert inventory.pods["uid-owned"].claim_name == "pending-claim"
    assert inventory.pods["uid-warm"].claim_name is None
    del api.objects[("sandboxclaims", "pending-claim")]
    after = _client(api).capacity_inventory(
        priority_class="acme-sandbox", request_timeout_seconds=1
    )
    assert after.claims == frozenset()
    assert after.sandboxes["owned-sandbox"] == "pending-claim"
    assert after.pods["uid-owned"].claim_name == "pending-claim"


@pytest.mark.parametrize("resource", ["sandboxclaims", "sandboxes", "pods"])
def test_incomplete_inventory_raises_instead_of_reporting_empty_capacity(resource: str) -> None:
    api = InventoryAPI()
    if resource == "pods":
        api.pod_list_error = kube.ApiException(status=403)
    else:
        api.failures[("list", resource)] = kube.ApiException(status=403)
    with pytest.raises(kube.ApiException):
        _client(api).capacity_inventory(priority_class="acme-sandbox", request_timeout_seconds=1)
