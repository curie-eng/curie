"""Resource exhaustion diagnosis and delivered limits (ADR 0269, #5427).

Every pod here is a recorded kubelet shape from ``recorded_pod_shapes.json``,
deserialized into the Kubernetes client's own models and read through the
production ``KubernetesSandboxClient``. Only the CoreV1Api and
CustomObjectsApi objects are doubles. A variant changes one field of a
recorded shape, in the format the kubelet writes, and says which.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from curie_worker.sandbox import k8s as k8s_module
from curie_worker.sandbox.affinity import AffinityStore
from curie_worker.sandbox.resources import claim_resources_spec
from curie_worker.sandbox.substrate import SandboxSubstrate
from curie_worker.sandbox.types import (
    AdmissionConfig,
    DeliveredLimits,
    ResourceExhaustion,
    SandboxTermination,
    SubstrateConfig,
)
from curie_worker.workitem_dispatch import ResourceGate
from kubernetes import client as k8s_client

from .conftest import FakeSandboxClient
from .recorded_pods import MI, POOL
from .recorded_pods import client as _client
from .recorded_pods import event as _event
from .recorded_pods import pod as _pod
from .recorded_pods import shape as _shape
from .recorded_pods import templates as _templates

OVERRIDE = {
    "requests": {"cpu": "50m", "memory": "32Mi", "ephemeral-storage": "16Mi"},
    "limits": {"cpu": "200m", "memory": "96Mi", "ephemeral-storage": "128Mi"},
}


class _Api:
    """The CoreV1Api and CustomObjectsApi boundary, nothing else."""

    def __init__(
        self,
        pod: Any = None,
        *,
        events: list[Any] | None = None,
        pod_error: BaseException | None = None,
        objects: dict[tuple[str, str], dict[str, Any]] | None = None,
    ) -> None:
        self.pod = pod
        self.events = events or []
        self.pod_error = pod_error
        self.objects = objects or {}
        self.pod_reads: list[str] = []

    def read_namespaced_pod(self, name: str, namespace: str, *, _request_timeout: float) -> Any:
        assert namespace == "test-ns" and _request_timeout > 0
        self.pod_reads.append(name)
        if self.pod_error is not None:
            raise self.pod_error
        return self.pod

    def list_namespaced_event(
        self, namespace: str, *, field_selector: str, limit: int, _request_timeout: float
    ) -> SimpleNamespace:
        del namespace, field_selector, limit, _request_timeout
        return SimpleNamespace(items=self.events)

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str, **_: Any
    ) -> dict[str, Any]:
        del group, version, namespace
        found = self.objects.get((plural, name))
        if found is None:
            raise k8s_client.ApiException(status=404, reason="NotFound")
        return copy.deepcopy(found)


def _terminate(shape: dict[str, Any], **api: Any) -> SandboxTermination | None:
    since = datetime.now(UTC) - timedelta(minutes=5)
    return _client(_Api(_pod(shape), **api)).pod_termination(
        shape["metadata"]["name"], since=since, request_timeout_seconds=0.5
    )


# -- positive identification from the recorded shapes -----------------------------


def test_recorded_oom_kill_names_the_runner_memory_limit() -> None:
    termination = _terminate(_shape("oom-probe"))

    assert termination is not None
    assert termination.reason == "OOMKilled"
    assert termination.exit_code == 137
    assert termination.exhaustion == ResourceExhaustion("memory", None, 48 * MI)


def test_recorded_emptydir_eviction_names_the_volume_and_its_size_limit() -> None:
    termination = _terminate(_shape("scratch-probe"))

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.exhaustion == ResourceExhaustion("scratch", "workspace", 8 * MI)


def test_recorded_pod_storage_eviction_names_the_containers_total() -> None:
    termination = _terminate(_shape("ephemeral-probe"))

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.exhaustion == ResourceExhaustion("ephemeral_storage", None, 12 * MI)


def test_runner_container_storage_eviction_names_the_pod_total() -> None:
    # The kubelet's container-level sentence on the recorded ephemeral pod.
    shape = _shape("ephemeral-probe")
    shape["status"]["message"] = (
        'Container runner exceeded its local ephemeral storage limit "12Mi". '
    )

    termination = _terminate(shape)

    assert termination is not None
    assert termination.exhaustion == ResourceExhaustion("ephemeral_storage", None, 12 * MI)


@pytest.mark.parametrize(
    ("spec_limit", "identified"),
    [("8388608", True), ("8Mi", True), ("8M", False), ("16Mi", False)],
)
def test_emptydir_limits_compare_as_quantities(spec_limit: str, identified: bool) -> None:
    # The recorded message says "8Mi"; only the spec sizeLimit changes.
    shape = _shape("scratch-probe")
    shape["spec"]["volumes"][0]["emptyDir"]["sizeLimit"] = spec_limit

    termination = _terminate(shape)

    assert termination is not None and termination.reason == "Evicted"
    expected = ResourceExhaustion("scratch", "workspace", 8 * MI) if identified else None
    assert termination.exhaustion == expected


@pytest.mark.parametrize(
    "reason", ["DeletionByTaintManager", "DeletionByPodGC", "EvictionByEvictionAPI"]
)
def test_external_disruption_target_is_a_node_disruption(reason: str) -> None:
    shape = _shape("ephemeral-probe")
    shape["status"].pop("reason")
    shape["status"].pop("message")
    shape["metadata"]["deletionTimestamp"] = datetime.now(UTC).isoformat()
    shape["status"]["conditions"].append(
        {"type": "DisruptionTarget", "status": "True", "reason": reason}
    )

    termination = _terminate(shape)

    assert termination is not None
    assert termination.reason == "Deleted"
    assert termination.exhaustion == ResourceExhaustion("node_disruption", None, None)


# -- refusals: nothing short of positive identification ---------------------------


@pytest.mark.parametrize(
    ("condition_status", "reason"),
    [
        ("True", "TerminationByKubelet"),
        ("True", "PreemptionByScheduler"),
        ("True", "SomethingNew"),
        ("False", "DeletionByTaintManager"),
    ],
)
def test_other_disruption_targets_identify_nothing(condition_status: str, reason: str) -> None:
    shape = _shape("ephemeral-probe")
    shape["status"].pop("reason")
    shape["status"].pop("message")
    shape["metadata"]["deletionTimestamp"] = datetime.now(UTC).isoformat()
    shape["status"]["conditions"].append(
        {"type": "DisruptionTarget", "status": condition_status, "reason": reason}
    )

    termination = _terminate(shape)

    assert termination is not None and termination.reason == "Deleted"
    assert termination.exhaustion is None


def test_an_oom_event_alone_identifies_nothing() -> None:
    # The pod is unreadable, so only the event remains: it names a reason,
    # never a limit.
    oom = _shape("oom-probe")
    event = _event(
        {
            "reason": "OOMKilling",
            "message": "Memory cgroup out of memory: Killed process 1 (python)",
            "type": "Warning",
            "involvedObject": {"kind": "Pod", "name": "oom-probe"},
        },
        uid=oom["metadata"]["uid"],
        at=datetime.now(UTC),
    )
    since = datetime.now(UTC) - timedelta(minutes=5)
    termination = _client(
        _Api(None, events=[event], pod_error=k8s_client.ApiException(status=403))
    ).pod_termination("oom-probe", since=since, request_timeout_seconds=0.5)

    assert termination is not None
    assert termination.reason == "OOMKilling"
    assert termination.exhaustion is None


def test_a_recorded_eviction_event_for_an_unreadable_pod_identifies_nothing() -> None:
    shape = _shape("scratch-probe")
    event = _event(shape["events"][0], uid=shape["metadata"]["uid"], at=datetime.now(UTC))
    since = datetime.now(UTC) - timedelta(minutes=5)
    termination = _client(
        _Api(None, events=[event], pod_error=k8s_client.ApiException(status=403))
    ).pod_termination("scratch-probe", since=since, request_timeout_seconds=0.5)

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.exhaustion is None


@pytest.mark.parametrize(
    ("shape_name", "message"),
    [
        # Node pressure: the kubelet's own generic eviction.
        ("scratch-probe", "The node was low on resource: memory. "),
        (
            "scratch-probe",
            "The node was low on resource: ephemeral-storage. Threshold quantity: 1Gi, "
            "available: 512Mi. ",
        ),
        # The EmptyDir sentence with a limit the spec does not carry.
        ("scratch-probe", 'Usage of EmptyDir volume "workspace" exceeds the limit "16Mi". '),
        # A volume the spec does not declare.
        ("scratch-probe", 'Usage of EmptyDir volume "cache" exceeds the limit "8Mi". '),
        # The sentence with trailing text the kubelet never writes.
        ("scratch-probe", 'Usage of EmptyDir volume "workspace" exceeds the limit "8Mi". x'),
        # A pod total that differs from the containers' sum.
        (
            "ephemeral-probe",
            "Pod ephemeral local storage usage exceeds the total limit of containers 24Mi. ",
        ),
        # Another container's own limit.
        (
            "ephemeral-probe",
            'Container sidecar exceeded its local ephemeral storage limit "12Mi". ',
        ),
        ("ephemeral-probe", None),
    ],
)
def test_evictions_that_name_no_matching_limit_identify_nothing(
    shape_name: str, message: str | None
) -> None:
    shape = _shape(shape_name)
    shape["status"]["message"] = message

    termination = _terminate(shape)

    assert termination is not None and termination.reason == "Evicted"
    assert termination.exhaustion is None


def test_a_volume_without_a_size_limit_identifies_nothing() -> None:
    shape = _shape("scratch-probe")
    shape["spec"]["volumes"][0]["emptyDir"] = {}

    termination = _terminate(shape)

    assert termination is not None and termination.exhaustion is None


def test_an_oom_kill_without_a_runner_memory_limit_identifies_nothing() -> None:
    shape = _shape("oom-probe")
    del shape["spec"]["containers"][0]["resources"]["limits"]["memory"]

    termination = _terminate(shape)

    assert termination is not None and termination.reason == "OOMKilled"
    assert termination.exhaustion is None


def test_a_replacement_pod_identifies_nothing() -> None:
    shape = _shape("oom-probe")
    shape["metadata"]["creationTimestamp"] = datetime.now(UTC).isoformat()

    termination = _terminate(shape)

    assert termination == SandboxTermination("Deleted", "replaced by a new pod with the same name")


def test_an_attribute_pod_that_is_no_client_model_identifies_nothing() -> None:
    # Older tests and odd clients hand back plain objects; they never read as a spec.
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="sbx-1", uid="uid-1"),
        spec=SimpleNamespace(containers=[]),
        status=SimpleNamespace(
            phase="Failed",
            reason="Evicted",
            message='Usage of EmptyDir volume "workspace" exceeds the limit "8Mi". ',
            container_statuses=[],
        ),
    )
    termination = _client(_Api(pod)).pod_termination(
        "sbx-1", since=datetime.now(UTC), request_timeout_seconds=0.5
    )

    assert termination is not None and termination.exhaustion is None


# -- delivered and template limits ------------------------------------------------


def test_pod_limits_read_the_recorded_specs() -> None:
    oom = _client(_Api(_pod(_shape("oom-probe")))).pod_limits(
        "oom-probe", request_timeout_seconds=1.0
    )
    scratch = _client(_Api(_pod(_shape("scratch-probe")))).pod_limits(
        "scratch-probe", request_timeout_seconds=1.0
    )

    assert oom == DeliveredLimits(
        memory_bytes=48 * MI, ephemeral_storage_bytes=64 * MI, scratch_bytes={}, max_turns=None
    )
    assert scratch == DeliveredLimits(
        memory_bytes=64 * MI,
        ephemeral_storage_bytes=64 * MI,
        scratch_bytes={"workspace": 8 * MI},
        max_turns=None,
    )


def test_pod_limits_of_an_unreadable_pod_are_unobserved() -> None:
    api = _Api(pod_error=k8s_client.ApiException(status=403))

    assert _client(api).pod_limits("scratch-probe", request_timeout_seconds=1.0) is None
    assert api.pod_reads == ["scratch-probe"]


def test_claim_limits_resolve_the_template_and_the_override() -> None:
    spec = _shape("scratch-probe")["spec"]
    client = _client(_Api(objects=_templates(spec)))

    plain = client.claim_limits(pool=POOL, runner_resources=None, agent_name=None)
    raised = client.claim_limits(pool=POOL, runner_resources=OVERRIDE, agent_name="acme-bot")

    assert plain == DeliveredLimits(64 * MI, 64 * MI, {"workspace": 8 * MI}, None)
    # The override replaces container resources only; it never moves a sizeLimit.
    assert raised == DeliveredLimits(96 * MI, 128 * MI, {"workspace": 8 * MI}, None)
    expected = claim_resources_spec({"podTemplate": {"spec": spec}}, OVERRIDE)
    assert expected["podTemplate"]["spec"]["volumes"] == spec["volumes"]


def test_claim_limits_refuse_an_unresolved_template() -> None:
    with pytest.raises(ValueError, match="source pool"):
        _client(_Api()).claim_limits(pool=POOL, runner_resources=None, agent_name=None)


def test_a_warm_pod_reports_its_own_spec_not_its_template() -> None:
    # The template moved to 64Mi after this warm pod was created at 48Mi.
    template = _shape("scratch-probe")["spec"]
    warm = _shape("oom-probe")
    client = _client(_Api(_pod(warm), objects=_templates(template)))

    pod = client.pod_limits("oom-probe", request_timeout_seconds=1.0)
    resolved = client.claim_limits(pool=POOL, runner_resources=None, agent_name=None)

    assert pod is not None and pod.memory_bytes == 48 * MI
    assert resolved.memory_bytes == 64 * MI


# -- substrate wrappers -----------------------------------------------------------


def _gate(dimension: str, *, volume: str | None = None) -> ResourceGate:
    return ResourceGate(
        condition=f"{dimension}_limit",
        dimension=dimension,  # type: ignore[arg-type]
        volume=volume,
        failed_limit=1,
        current_limit=None,
    )


@pytest.fixture
def template_substrate(
    affinity: AffinityStore, config: SubstrateConfig
) -> Iterator[tuple[SandboxSubstrate, _Api]]:
    api = _Api(objects=_templates(_shape("scratch-probe")["spec"]))
    yield SandboxSubstrate(_client(api), affinity, config), api


def _readiness(
    substrate: SandboxSubstrate, gate: ResourceGate, resources: dict[str, Any] | None = None
) -> int | None:
    return substrate.resource_gate_readiness(
        gate,
        env=None,
        workspace_repo=None,
        agent_name="acme-bot" if resources is not None else None,
        runner_resources=resources,
    )


def test_readiness_reads_the_gated_dimension_of_the_resolved_template(
    template_substrate: tuple[SandboxSubstrate, _Api],
) -> None:
    substrate, _api = template_substrate

    assert _readiness(substrate, _gate("memory")) == 64 * MI
    assert _readiness(substrate, _gate("memory"), OVERRIDE) == 96 * MI
    assert _readiness(substrate, _gate("ephemeral_storage"), OVERRIDE) == 128 * MI
    assert _readiness(substrate, _gate("scratch", volume="workspace")) == 8 * MI
    assert _readiness(substrate, _gate("scratch", volume="cache")) is None


def test_readiness_of_an_unresolved_template_is_none(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    substrate = SandboxSubstrate(_client(_Api()), affinity, config)

    assert _readiness(substrate, _gate("memory")) is None


@pytest.mark.parametrize(("hard_memory", "ready"), [("64Mi", False), ("1Gi", True)])
def test_readiness_needs_the_footprint_to_fit_the_admission_ceiling(
    affinity: AffinityStore, config: SubstrateConfig, hard_memory: str, ready: bool
) -> None:
    from dataclasses import replace

    admission = AdmissionConfig(
        quota_name="sandbox-quota",
        priority_class="sandbox",
        ceiling=4,
        hard={
            "pods": "4",
            "requests.cpu": "1",
            "requests.memory": "1Gi",
            "limits.cpu": "1",
            "limits.memory": hard_memory,
        },
    )
    api = _Api(objects=_templates(_shape("scratch-probe")["spec"]))
    substrate = SandboxSubstrate(_client(api), affinity, replace(config, admission=admission))

    expected = 96 * MI if ready else None
    assert _readiness(substrate, _gate("memory"), OVERRIDE) == expected


def test_delivered_limits_of_an_unreadable_pod_are_null_with_the_boot_turns(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    fake = FakeSandboxClient()
    substrate = SandboxSubstrate(fake, affinity, config)
    handle = substrate.claim("thread-limits")

    unread = substrate.delivered_limits(handle, max_turns=40)
    fake.delivered = DeliveredLimits(48 * MI, 64 * MI, {"workspace": 8 * MI}, None)
    read = substrate.delivered_limits(handle, max_turns=40)

    assert unread == DeliveredLimits(None, None, {}, 40)
    assert read == DeliveredLimits(48 * MI, 64 * MI, {"workspace": 8 * MI}, 40)
    assert fake.pod_limit_reads == [handle.sandbox_name, handle.sandbox_name]


def test_predecessor_fence_releases_a_route_it_names_and_confirms_absence(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    fake = FakeSandboxClient()
    substrate = SandboxSubstrate(fake, affinity, config)
    handle = substrate.claim("thread-fence")

    assert substrate.predecessor_absent("thread-fence", handle.claim_name) is True
    assert handle.claim_name not in fake.claims
    assert affinity.get("thread-fence") is None


def test_predecessor_fence_refuses_a_lingering_claim_no_route_names(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    fake = FakeSandboxClient()
    substrate = SandboxSubstrate(fake, affinity, config)
    handle = substrate.claim("thread-old")
    # The route lapsed, but the claim object is still there.
    affinity.delete_if_claim("thread-old", handle.claim_name)

    assert substrate.predecessor_absent("thread-new", handle.claim_name) is False
    assert handle.claim_name in fake.claims
    fake.delete_claim(handle.claim_name, request_timeout_seconds=1.0)
    assert substrate.predecessor_absent("thread-new", handle.claim_name) is True


def test_predecessor_fence_treats_an_unreadable_claim_as_present(
    affinity: AffinityStore, config: SubstrateConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeSandboxClient()
    substrate = SandboxSubstrate(fake, affinity, config)

    def unreadable(name: str, *, request_timeout_seconds: float) -> None:
        raise k8s_client.ApiException(status=500)

    monkeypatch.setattr(fake, "get_claim", unreadable)
    assert substrate.predecessor_absent("thread-x", "curie-thread-gone") is False


def test_module_diagnosis_reads_only_the_pod_status() -> None:
    # The helper takes the status reason the same pod produced; events never reach it.
    shape = _shape("scratch-probe")
    assert k8s_module._pod_exhaustion(_pod(shape), "Evicted") == ResourceExhaustion(
        "scratch", "workspace", 8 * MI
    )
    assert k8s_module._pod_exhaustion(_pod(shape), "Error") is None
