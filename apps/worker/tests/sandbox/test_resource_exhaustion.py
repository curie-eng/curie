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
from curie_worker.sandbox.resources import claim_resources_spec, spec_limits
from curie_worker.sandbox.substrate import SandboxSubstrate
from curie_worker.sandbox.types import (
    AdmissionConfig,
    DeliveredLimits,
    RecordedPod,
    ResourceExhaustion,
    SandboxHandle,
    SandboxTermination,
    SubstrateConfig,
)
from curie_worker.workitem_dispatch import ResourceGate
from kubernetes import client as k8s_client

from .conftest import FakeSandboxClient
from .recorded_pods import MI, POOL
from .recorded_pods import client as _client
from .recorded_pods import event as _event
from .recorded_pods import event_page as _event_page
from .recorded_pods import pod as _pod
from .recorded_pods import shape as _shape
from .recorded_pods import startup_events as _startup_events
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
        self.event_selectors: list[str] = []

    def read_namespaced_pod(self, name: str, namespace: str, *, _request_timeout: float) -> Any:
        assert namespace == "test-ns" and _request_timeout > 0
        self.pod_reads.append(name)
        if self.pod_error is not None:
            raise self.pod_error
        return self.pod

    def list_namespaced_event(
        self, namespace: str, *, field_selector: str, limit: int, _request_timeout: float
    ) -> Any:
        assert namespace == "test-ns" and limit > 0 and _request_timeout > 0
        self.event_selectors.append(field_selector)
        return _event_page(self.events, field_selector=field_selector, limit=limit)

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


def _recorded(shape: dict[str, Any]) -> RecordedPod:
    """The identity the worker read at start: the shape's UID and wire spec."""

    return RecordedPod(uid=shape["metadata"]["uid"], spec=copy.deepcopy(shape["spec"]))


def _eviction(
    shape: dict[str, Any],
    *,
    message: str | None = None,
    reason: str = "Evicted",
    uid: str | None = None,
    at: datetime | None = None,
) -> Any:
    """The shape's recorded Evicted event, about ``uid`` (the shape's own by default)."""

    raw = dict(shape["events"][0])
    raw["reason"] = reason
    if message is not None:
        raw["message"] = message
    return _event(
        raw,
        uid=shape["metadata"]["uid"] if uid is None else uid,
        at=datetime.now(UTC) if at is None else at,
    )


def _replacement(shape: dict[str, Any]) -> dict[str, Any]:
    """The controller's new pod under the same name: created now, another UID."""

    replacement = copy.deepcopy(shape)
    replacement["metadata"]["creationTimestamp"] = datetime.now(UTC).isoformat()
    replacement["metadata"]["uid"] = "00000000-0000-0000-0000-0000000000ff"
    replacement["status"] = {"phase": "Pending"}
    return replacement


def _after_start(
    shape: dict[str, Any],
    pod_state: str,
    events: list[Any],
    recorded: RecordedPod | None,
    since: datetime | None = None,
) -> SandboxTermination | None:
    """Diagnose ``shape``'s name once the pod is ``missing``, ``unreadable`` or ``replaced``."""

    if pod_state == "missing":
        api = _Api(None, events=events, pod_error=k8s_client.ApiException(status=404))
    elif pod_state == "unreadable":
        api = _Api(None, events=events, pod_error=k8s_client.ApiException(status=403))
    else:
        assert pod_state == "replaced"
        api = _Api(_pod(_replacement(shape)), events=events)
    return _client(api).pod_termination(
        shape["metadata"]["name"],
        since=since or datetime.now(UTC) - timedelta(minutes=5),
        request_timeout_seconds=0.5,
        recorded=recorded,
    )


@pytest.mark.parametrize("pod_state", ["missing", "unreadable"])
@pytest.mark.parametrize(
    ("shape_name", "exhaustion"),
    [
        ("scratch-probe", ResourceExhaustion("scratch", "workspace", 8 * MI)),
        ("ephemeral-probe", ResourceExhaustion("ephemeral_storage", None, 12 * MI)),
    ],
)
def test_a_kubelet_eviction_event_bound_to_the_recorded_pod_identifies_the_storage_limit(
    shape_name: str, exhaustion: ResourceExhaustion, pod_state: str
) -> None:
    # The evicted pod is gone (404) or unreadable (403): only the kubelet's own
    # Evicted event is left, and it is about the UID the worker recorded at
    # start, stating the limit the recorded spec declares.
    shape = _shape(shape_name)

    termination = _after_start(shape, pod_state, [_eviction(shape)], _recorded(shape))

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.exhaustion == exhaustion


@pytest.mark.parametrize(
    ("shape_name", "exhaustion"),
    [
        ("scratch-probe", ResourceExhaustion("scratch", "workspace", 8 * MI)),
        ("ephemeral-probe", ResourceExhaustion("ephemeral_storage", None, 12 * MI)),
    ],
)
def test_a_replacement_pod_leaves_the_recorded_pods_eviction_identified(
    shape_name: str, exhaustion: ResourceExhaustion
) -> None:
    # The controller replaced the evicted pod before the read; the event still
    # names the original, recorded UID.
    shape = _shape(shape_name)

    termination = _after_start(shape, "replaced", [_eviction(shape)], _recorded(shape))

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.exhaustion == exhaustion


# The init containers a production sandbox template runs ahead of the runner.
_INIT_CONTAINERS = ["init-0", "init-1", "init-2", "init-3"]


@pytest.mark.parametrize("pod_state", ["missing", "replaced"])
@pytest.mark.parametrize(
    ("shape_name", "exhaustion"),
    [
        ("scratch-probe", ResourceExhaustion("scratch", "workspace", 8 * MI)),
        ("ephemeral-probe", ResourceExhaustion("ephemeral_storage", None, 12 * MI)),
    ],
)
def test_an_eviction_past_the_first_page_of_the_pods_events_is_still_identified(
    shape_name: str, exhaustion: ResourceExhaustion, pod_state: str
) -> None:
    # Each container's Pulling, Pulled, Created and Started put more than one
    # page of events ahead of the kubelet's Evicted event under the pod's name,
    # and the replacement's own startup follows it.
    shape = _shape(shape_name)
    name = shape["metadata"]["name"]
    since = datetime.now(UTC) - timedelta(minutes=5)
    containers = _INIT_CONTAINERS + [container["name"] for container in shape["spec"]["containers"]]
    started = _startup_events(
        name, containers, uid=shape["metadata"]["uid"], start=since + timedelta(seconds=1)
    )
    replacement = _startup_events(
        name,
        containers,
        uid=_replacement(shape)["metadata"]["uid"],
        start=since + timedelta(seconds=90),
    )
    events = [*started, _eviction(shape, at=since + timedelta(seconds=60)), *replacement]
    first_page = _event_page(
        events, field_selector=f"involvedObject.kind=Pod,involvedObject.name={name}", limit=20
    )
    assert len(started) > 20
    assert "Evicted" not in {item.reason for item in first_page.items}

    termination = _after_start(shape, pod_state, events, _recorded(shape), since=since)

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.exhaustion == exhaustion


def test_a_replacement_pod_without_a_recorded_identity_identifies_nothing() -> None:
    shape = _shape("scratch-probe")

    termination = _after_start(shape, "replaced", [_eviction(shape)], None)

    assert termination == SandboxTermination("Deleted", "replaced by a new pod with the same name")


def _scratch_spec(size_limit: str) -> RecordedPod:
    shape = _shape("scratch-probe")
    shape["spec"]["volumes"][0]["emptyDir"]["sizeLimit"] = size_limit
    return _recorded(shape)


_SCRATCH = _shape("scratch-probe")
_EPHEMERAL = _shape("ephemeral-probe")
# case -> (shape, event fields, recorded: None = the shape's own, "none" = no identity)
_UNBOUND_EVICTIONS: dict[str, tuple[dict[str, Any], dict[str, Any], Any]] = {
    "another uid": (_SCRATCH, {"uid": "00000000-0000-0000-0000-0000000000aa"}, None),
    "event states another limit": (
        _SCRATCH,
        {"message": 'Usage of EmptyDir volume "workspace" exceeds the limit "16Mi". '},
        None,
    ),
    "recorded spec states another limit": (_SCRATCH, {}, _scratch_spec("16Mi")),
    "node pressure": (
        _SCRATCH,
        {
            "message": "The node was low on resource: ephemeral-storage. "
            "Threshold quantity: 1Gi, available: 512Mi. "
        },
        None,
    ),
    "trailing text": (
        _SCRATCH,
        {"message": 'Usage of EmptyDir volume "workspace" exceeds the limit "8Mi". x'},
        None,
    ),
    "volume the recorded spec lacks": (
        _SCRATCH,
        {"message": 'Usage of EmptyDir volume "cache" exceeds the limit "8Mi". '},
        None,
    ),
    "pod total the recorded spec does not sum to": (
        _EPHEMERAL,
        {
            "message": "Pod ephemeral local storage usage exceeds the total limit "
            "of containers 24Mi. "
        },
        None,
    ),
    "no recorded identity": (_SCRATCH, {}, "none"),
    "an oom event": (
        _SCRATCH,
        {"reason": "OOMKilling", "message": "Memory cgroup out of memory: Killed process 1"},
        None,
    ),
    "older than since": (_SCRATCH, {"at": datetime.now(UTC) - timedelta(minutes=10)}, None),
}


@pytest.mark.parametrize("pod_state", ["missing", "replaced"])
@pytest.mark.parametrize("case", list(_UNBOUND_EVICTIONS))
def test_an_eviction_event_not_bound_to_the_recorded_pod_and_limit_identifies_nothing(
    case: str, pod_state: str
) -> None:
    template, event_fields, recorded_override = _UNBOUND_EVICTIONS[case]
    shape = copy.deepcopy(template)
    if recorded_override == "none":
        recorded = None
    elif recorded_override is None:
        recorded = _recorded(shape)
    else:
        recorded = recorded_override

    termination = _after_start(shape, pod_state, [_eviction(shape, **event_fields)], recorded)

    assert termination is not None
    assert termination.exhaustion is None
    if pod_state == "replaced":
        assert termination == SandboxTermination(
            "Deleted", "replaced by a new pod with the same name"
        )


def test_the_pods_own_oom_kill_outranks_a_bound_storage_eviction() -> None:
    shape = _shape("oom-probe")
    event = _eviction(
        _shape("ephemeral-probe"),
        message="Pod ephemeral local storage usage exceeds the total limit of containers 64Mi. ",
        uid=shape["metadata"]["uid"],
    )
    event.involved_object.name = "oom-probe"

    termination = _client(_Api(_pod(shape), events=[event])).pod_termination(
        "oom-probe",
        since=datetime.now(UTC) - timedelta(minutes=5),
        request_timeout_seconds=0.5,
        recorded=_recorded(shape),
    )

    assert termination is not None
    assert termination.reason == "OOMKilled"
    assert termination.exhaustion == ResourceExhaustion("memory", None, 48 * MI)


def test_a_bound_eviction_identifies_a_recorded_pod_not_yet_terminal() -> None:
    # The kubelet's event can land before the pod's own status turns Failed.
    shape = _shape("scratch-probe")
    running = copy.deepcopy(shape)
    running["status"] = {"phase": "Running"}

    termination = _client(_Api(_pod(running), events=[_eviction(shape)])).pod_termination(
        "scratch-probe",
        since=datetime.now(UTC) - timedelta(minutes=5),
        request_timeout_seconds=0.5,
        recorded=_recorded(shape),
    )

    assert termination is not None
    assert termination.reason == "Evicted"
    assert termination.exhaustion == ResourceExhaustion("scratch", "workspace", 8 * MI)


def test_an_eviction_event_for_an_unreadable_pod_without_a_recorded_identity_names_nothing() -> (
    None
):
    shape = _shape("scratch-probe")

    termination = _after_start(shape, "unreadable", [_eviction(shape)], None)

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


def test_pod_limits_record_the_pods_uid_and_wire_spec() -> None:
    shape = _shape("scratch-probe")

    limits = _client(_Api(_pod(shape))).pod_limits("scratch-probe", request_timeout_seconds=1.0)

    assert limits is not None and limits.recorded is not None
    assert limits.recorded.uid == shape["metadata"]["uid"]
    assert limits.recorded.spec["volumes"] == shape["spec"]["volumes"]
    assert spec_limits(limits.recorded.spec) == limits
    assert "recorded" not in limits.to_wire()


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


def _handle(claim_name: str, sandbox_name: str) -> SandboxHandle:
    return SandboxHandle(
        thread_key="thread-evicted",
        claim_name=claim_name,
        sandbox_name=sandbox_name,
        namespace="test-ns",
        service_fqdn=f"{sandbox_name}.test-ns.svc.cluster.local",
        port=8080,
        session_id="session",
    )


@pytest.mark.parametrize(
    ("diagnosed_claim", "exhaustion"),
    [
        ("claim-a", ResourceExhaustion("scratch", "workspace", 8 * MI)),
        # The same sandbox name under another claim is another pod's runtime.
        ("claim-b", None),
    ],
)
def test_delivered_limits_record_the_pod_that_a_later_diagnosis_binds_to(
    affinity: AffinityStore,
    config: SubstrateConfig,
    diagnosed_claim: str,
    exhaustion: ResourceExhaustion | None,
) -> None:
    shape = _shape("scratch-probe")
    api = _Api(_pod(shape), events=[_eviction(shape)])
    substrate = SandboxSubstrate(_client(api), affinity, config)
    started = _handle("claim-a", "scratch-probe")

    limits = substrate.delivered_limits(started, max_turns=40)
    # The kubelet evicted the pod and the controller deleted it.
    api.pod_error = k8s_client.ApiException(status=404)
    termination = substrate.pod_termination(
        _handle(diagnosed_claim, "scratch-probe"),
        since=datetime.now(UTC) - timedelta(minutes=5),
    )

    assert limits == DeliveredLimits(64 * MI, 64 * MI, {"workspace": 8 * MI}, 40)
    assert limits.recorded is None
    assert termination is not None and termination.reason == "Evicted"
    assert termination.exhaustion == exhaustion


def test_an_unreadable_pod_at_start_drops_the_earlier_record(
    affinity: AffinityStore, config: SubstrateConfig
) -> None:
    shape = _shape("scratch-probe")
    api = _Api(_pod(shape), events=[_eviction(shape)])
    substrate = SandboxSubstrate(_client(api), affinity, config)
    handle = _handle("claim-a", "scratch-probe")

    substrate.delivered_limits(handle, max_turns=None)
    api.pod_error = k8s_client.ApiException(status=403)
    substrate.delivered_limits(handle, max_turns=None)
    api.pod_error = k8s_client.ApiException(status=404)
    termination = substrate.pod_termination(handle, since=datetime.now(UTC) - timedelta(minutes=5))

    assert termination is not None and termination.exhaustion is None
